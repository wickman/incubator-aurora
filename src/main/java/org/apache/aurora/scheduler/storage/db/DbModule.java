/**
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
package org.apache.aurora.scheduler.storage.db;

import java.util.Properties;

import javax.inject.Singleton;

import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Key;
import com.google.inject.PrivateModule;
import com.google.inject.name.Names;
import com.twitter.common.inject.Bindings;

import org.apache.aurora.scheduler.storage.AttributeStore;
import org.apache.aurora.scheduler.storage.LockStore;
import org.apache.aurora.scheduler.storage.QuotaStore;
import org.apache.aurora.scheduler.storage.SchedulerStore;
import org.apache.aurora.scheduler.storage.Storage;
import org.apache.aurora.scheduler.storage.db.typehandlers.TypeHandlers;
import org.apache.ibatis.session.AutoMappingBehavior;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.ibatis.transaction.jdbc.JdbcTransactionFactory;
import org.h2.Driver;
import org.mybatis.guice.MyBatisModule;
import org.mybatis.guice.datasource.builtin.PooledDataSourceProvider;

import static java.util.Objects.requireNonNull;

import static com.google.inject.name.Names.named;

/**
 * Binding module for a relational database storage system.
 * <p>
 *   Currently only exposes bindings for:
 *   <ul>
 *     <li>{@link org.apache.aurora.scheduler.storage.db.DbStorage}</li>
 *     <li>{@link org.apache.ibatis.session.SqlSessionFactory}</li>
 *     <li>Keys provided by the provided{@code keyFactory} for:
 *        <ul>
 *          <li>{@link LockStore.Mutable}</li>
 *          <li>{@link QuotaStore.Mutable}</li>
 *          <li>{@link SchedulerStore.Mutable}</li>
 *        </ul>
 *     </li>
 *   </ul>
 * </p>
 */
public class DbModule extends PrivateModule {

  private final Bindings.KeyFactory keyFactory;
  private final String jdbcUrl;

  private DbModule(Bindings.KeyFactory keyFactory, String jdbcUrl) {
    this.keyFactory = requireNonNull(keyFactory);
    this.jdbcUrl = requireNonNull(jdbcUrl);
  }

  public DbModule(Bindings.KeyFactory keyFactory) {
    this(keyFactory, "jdbc:h2:mem:aurora;DB_CLOSE_DELAY=-1");
  }

  /**
   * Creates a module that will prepare a private in-memory database.
   *
   * @param keyFactory Key factory to scope bindings.
   * @return A new database module for testing.
   */
  @VisibleForTesting
  public static DbModule testModule(Bindings.KeyFactory keyFactory) {
    // This creates a private in-memory database.  New connections will have a _new_ database,
    // and closing the database will expunge its data.
    return new DbModule(keyFactory, "jdbc:h2:mem:");
  }

  private <T> void bindStore(Class<T> binding, Class<? extends T> impl) {
    bind(binding).to(impl);
    bind(impl).in(Singleton.class);
    Key<T> key = keyFactory.create(binding);
    bind(key).to(impl);
    expose(key);
  }

  @Override
  protected void configure() {
    install(new MyBatisModule() {
      @Override
      protected void initialize() {
        // Ideally, we would install h2 from org.mybatis.guice.datasource.helper.JdbcHelper
        //     install(JdbcHelper.H2_IN_MEMORY_PRIVATE);
        // But the in-memory URL is invalid as far as H2 is concerned, so we had to inline
        // some of the constants here and bind it manually.
        bindConstant().annotatedWith(named("JDBC.driver")).to(Driver.class.getName());
        bind(Key.get(String.class, named("JDBC.url"))).toInstance(jdbcUrl);

        bindDataSourceProviderType(PooledDataSourceProvider.class);
        bindTransactionFactoryType(JdbcTransactionFactory.class);
        addMapperClass(AttributeMapper.class);
        addMapperClass(EnumValueMapper.class);
        addMapperClass(LockMapper.class);
        addMapperClass(JobKeyMapper.class);
        addMapperClass(QuotaMapper.class);
        addMapperClass(FrameworkIdMapper.class);
        Properties props = new Properties();
        // We have no plans to take advantage of multiple DB environments. This is a required
        // property though, so we use an unnamed environment.
        props.setProperty("mybatis.environment.id", "");
        Names.bindProperties(binder(), props);
        // Full auto-mapping enables population of nested objects with minimal mapper configuration.
        // Docs on settings can be found here:
        // http://mybatis.github.io/mybatis-3/configuration.html#settings
        autoMappingBehavior(AutoMappingBehavior.FULL);

        addTypeHandlersClasses(TypeHandlers.getAll());

        // TODO(davmclau): ensure that mybatis logging is configured correctly.
      }
    });
    bindStore(AttributeStore.Mutable.class, DbAttributeStore.class);
    bindStore(LockStore.Mutable.class, DbLockStore.class);
    bindStore(QuotaStore.Mutable.class, DbQuotaStore.class);
    bindStore(SchedulerStore.Mutable.class, DbSchedulerStore.class);

    Key<Storage> storageKey = keyFactory.create(Storage.class);
    bind(storageKey).to(DbStorage.class);
    bind(DbStorage.class).in(Singleton.class);
    expose(storageKey);

    expose(DbStorage.class);
    expose(SqlSessionFactory.class);
  }
}
