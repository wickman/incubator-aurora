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

import java.text.ParseException;
import java.util.Date;

import javax.inject.Inject;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.twitter.common.util.Clock;

import org.apache.aurora.scheduler.cron.CronPredictor;
import org.quartz.CronExpression;

class QuartzCronPredictor implements CronPredictor {
  private final Clock clock;

  @Inject
  public QuartzCronPredictor(Clock clock) {
    this.clock = Preconditions.checkNotNull(clock);
  }

  @Override
  public Date predictNextRun(String schedule) {
    try {
      CronExpression cronExpression = new CronExpression(schedule);
      return cronExpression.getNextValidTimeAfter(new Date(clock.nowMillis()));
    } catch (ParseException e) {
      throw Throwables.propagate(e);
    }
  }
}
