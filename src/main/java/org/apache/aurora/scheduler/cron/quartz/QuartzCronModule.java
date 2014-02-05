/*
 * Copyright 2014 Apache Software Foundation
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
package org.apache.aurora.scheduler.cron.quartz;

import java.util.UUID;
import java.util.logging.Logger;

import javax.inject.Singleton;

import com.google.common.base.Throwables;
import com.google.inject.PrivateModule;
import com.google.inject.Provides;

import org.apache.aurora.scheduler.cron.CronPredictor;
import org.apache.aurora.scheduler.cron.CronScheduler;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.impl.DirectSchedulerFactory;
import org.quartz.simpl.RAMJobStore;
import org.quartz.simpl.SimpleThreadPool;
import org.quartz.spi.ThreadPool;

/**
 * Created by ksweeney on 2/3/14.
 */
public class QuartzCronModule extends PrivateModule {
  private static final Logger LOG = Logger.getLogger(QuartzCronModule.class.getName());

  // TODO(kevints): Consider making this configurable if the global write lock goes away.
  private static final int NUM_THREADS = 1;

  @Override
  protected void configure() {
    bind(CronScheduler.class).to(QuartzCronScheduler.class);
    bind(QuartzCronScheduler.class).in(Singleton.class);

    bind(CronPredictor.class).to(QuartzCronPredictor.class);
    bind(QuartzCronPredictor.class).in(Singleton.class);

    expose(CronPredictor.class);
    expose(CronScheduler.class);
  }

  /*
   * XXX: Quartz implements DirectSchedulerFactory as a mutable global singleton in a static
   * variable. While the Scheduler instances it produces are independent we synchronize here to
   * avoid an initialization race across injectors. In practice this only shows up during testing;
   * production Aurora instances will only have one object graph at a time.
   */
  @Provides
  private static synchronized Scheduler provideScheduler(ThreadPool threadPool) {
    DirectSchedulerFactory schedulerFactory = DirectSchedulerFactory.getInstance();
    String schedulerName = "aurora-cron-" + UUID.randomUUID().toString();
    try {
      schedulerFactory.createScheduler(schedulerName, schedulerName, threadPool, new RAMJobStore());
      return schedulerFactory.getScheduler(schedulerName);
    } catch (SchedulerException e) {
      LOG.severe("Error initializing Quartz cron scheduler: " + e);
      throw Throwables.propagate(e);
    }
  }

  @Provides
  private ThreadPool provideThreadPool() {
    SimpleThreadPool simpleThreadPool = new SimpleThreadPool(NUM_THREADS, Thread.NORM_PRIORITY);
    simpleThreadPool.setMakeThreadsDaemons(true);
    return simpleThreadPool;
  }
}
