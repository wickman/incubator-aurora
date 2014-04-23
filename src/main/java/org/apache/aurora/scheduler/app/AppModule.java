/**
 * Copyright 2013 Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.aurora.scheduler.app;

import java.util.Arrays;
import java.util.List;
import java.util.logging.Logger;

import javax.inject.Inject;
import javax.inject.Singleton;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.Iterables;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.twitter.common.application.ShutdownRegistry;
import com.twitter.common.application.modules.LifecycleModule;
import com.twitter.common.base.Command;
import com.twitter.common.inject.TimedInterceptor;
import com.twitter.common.net.pool.DynamicHostSet;
import com.twitter.common.stats.Stats;
import com.twitter.common.stats.StatsProvider;
import com.twitter.common.util.Clock;
import com.twitter.common.zookeeper.ServerSet;
import com.twitter.common.zookeeper.ServerSetImpl;
import com.twitter.common.zookeeper.SingletonService;
import com.twitter.common.zookeeper.ZooKeeperClient;
import com.twitter.common.zookeeper.ZooKeeperClient.Credentials;
import com.twitter.common.zookeeper.ZooKeeperUtils;
import com.twitter.common.zookeeper.guice.client.ZooKeeperClientModule.ClientConfig;
import com.twitter.thrift.ServiceInstance;

import org.apache.aurora.GuiceUtils;
import org.apache.aurora.gen.ServerInfo;
import org.apache.aurora.scheduler.SchedulerModule;
import org.apache.aurora.scheduler.async.AsyncModule;
import org.apache.aurora.scheduler.events.PubsubEventModule;
import org.apache.aurora.scheduler.filter.SchedulingFilterImpl;
import org.apache.aurora.scheduler.http.ServletModule;
import org.apache.aurora.scheduler.metadata.MetadataModule;
import org.apache.aurora.scheduler.quota.QuotaModule;
import org.apache.aurora.scheduler.state.StateModule;
import org.apache.aurora.scheduler.stats.AsyncStatsModule;
import org.apache.aurora.scheduler.storage.entities.IServerInfo;
import org.apache.mesos.Scheduler;
import org.apache.zookeeper.data.ACL;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.twitter.common.base.MorePreconditions.checkNotBlank;

import static org.apache.aurora.gen.apiConstants.THRIFT_API_VERSION;

/**
 * Binding module for the aurora scheduler application.
 */
class AppModule extends AbstractModule {
  private static final Logger LOG = Logger.getLogger(AppModule.class.getName());

  private final String clusterName;
  private final String serverSetPath;
  private final String statsUrlPrefix;
  private final ClientConfig zkClientConfig;

  AppModule(
      String clusterName,
      String serverSetPath,
      ClientConfig zkClientConfig,
      String statsUrlPrefix) {

    this.clusterName = checkNotBlank(clusterName);
    this.serverSetPath = checkNotBlank(serverSetPath);
    this.statsUrlPrefix = statsUrlPrefix;
    this.zkClientConfig = checkNotNull(zkClientConfig);
  }

  @Override
  protected void configure() {
    // Enable intercepted method timings and context classloader repair.
    TimedInterceptor.bind(binder());
    GuiceUtils.bindJNIContextClassLoader(binder(), Scheduler.class);
    GuiceUtils.bindExceptionTrap(binder(), Scheduler.class);

    bind(Clock.class).toInstance(Clock.SYSTEM_CLOCK);

    bind(IServerInfo.class).toInstance(
        IServerInfo.build(
            new ServerInfo()
                .setClusterName(clusterName)
                .setThriftAPIVersion(THRIFT_API_VERSION)
                .setStatsUrlPrefix(statsUrlPrefix)));

    // Filter layering: notifier filter -> base impl
    PubsubEventModule.bind(binder(), SchedulingFilterImpl.class);
    bind(SchedulingFilterImpl.class).in(Singleton.class);

    LifecycleModule.bindStartupAction(binder(), RegisterShutdownStackPrinter.class);

    install(new AsyncModule());
    install(new AsyncStatsModule());
    install(new MetadataModule());
    install(new QuotaModule());
    install(new ServletModule());
    install(new SchedulerModule());
    install(new StateModule());

    bind(StatsProvider.class).toInstance(Stats.STATS_PROVIDER);
  }

  /**
   * Command to register a thread stack printer that identifies initiator of a shutdown.
   */
  private static class RegisterShutdownStackPrinter implements Command {
    private static final Function<StackTraceElement, String> STACK_ELEM_TOSTRING =
        new Function<StackTraceElement, String>() {
          @Override
          public String apply(StackTraceElement element) {
            return element.getClassName() + "." + element.getMethodName()
                + String.format("(%s:%s)", element.getFileName(), element.getLineNumber());
          }
        };

    private final ShutdownRegistry shutdownRegistry;

    @Inject
    RegisterShutdownStackPrinter(ShutdownRegistry shutdownRegistry) {
      this.shutdownRegistry = shutdownRegistry;
    }

    @Override
    public void execute() {
      shutdownRegistry.addAction(new Command() {
        @Override
        public void execute() {
          Thread thread = Thread.currentThread();
          String message = new StringBuilder()
              .append("Thread: ").append(thread.getName())
              .append(" (id ").append(thread.getId()).append(")")
              .append("\n")
              .append(Joiner.on("\n  ").join(
                  Iterables.transform(Arrays.asList(thread.getStackTrace()), STACK_ELEM_TOSTRING)))
              .toString();

          LOG.info("Shutdown initiated by: " + message);
        }
      });
    }
  }

  @Provides
  @Singleton
  List<ACL> provideAcls() {
    if (zkClientConfig.credentials == Credentials.NONE) {
      LOG.warning("Running without ZooKeeper digest credentials. ZooKeeper ACLs are disabled.");
      return ZooKeeperUtils.OPEN_ACL_UNSAFE;
    } else {
      return ZooKeeperUtils.EVERYONE_READ_CREATOR_ALL;
    }
  }

  @Provides
  @Singleton
  ServerSet provideServerSet(ZooKeeperClient client, List<ACL> zooKeeperAcls) {
    return new ServerSetImpl(client, zooKeeperAcls, serverSetPath);
  }

  @Provides
  @Singleton
  DynamicHostSet<ServiceInstance> provideSchedulerHostSet(ServerSet serverSet) {
    // Used for a type re-binding of the serverset.
    return serverSet;
  }

  @Provides
  @Singleton
  SingletonService provideSingletonService(
      ZooKeeperClient client,
      ServerSet serverSet,
      List<ACL> zookeeperAcls) {

    return new SingletonService(
        serverSet,
        SingletonService.createSingletonCandidate(client, serverSetPath, zookeeperAcls));
  }
}
