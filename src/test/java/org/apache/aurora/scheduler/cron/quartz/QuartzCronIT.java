/**
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

import java.util.Collection;

import com.google.common.collect.ImmutableSet;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.twitter.common.util.Clock;

import org.apache.aurora.scheduler.cron.CronPredictor;
import org.apache.aurora.scheduler.cron.CronScheduler;
import org.apache.aurora.scheduler.cron.testing.AbstractCronIT;

/**
 * Created by ksweeney on 2/4/14.
 */
public class QuartzCronIT extends AbstractCronIT {
  private static final String WILDCARD_SCHEDULE = "* * * * * ?";

  private Injector makeInjector() {
    return Guice.createInjector(new QuartzCronModule(), new AbstractModule() {
      @Override protected void configure() {
        bind(Clock.class).toInstance(Clock.SYSTEM_CLOCK);
      }
    });
  }

  @Override
  protected CronScheduler makeCronScheduler() throws Exception {
    return makeInjector().getInstance(CronScheduler.class);
  }

  @Override
  protected Collection<String> getValidCronSchedules() {
    return ImmutableSet.of(WILDCARD_SCHEDULE);
  }

  @Override
  protected String getWildcardCronSchedule() {
    return WILDCARD_SCHEDULE;
  }

  @Override
  protected CronPredictor makeCronPredictor() throws Exception {
    return makeInjector().getInstance(CronPredictor.class);
  }
}
