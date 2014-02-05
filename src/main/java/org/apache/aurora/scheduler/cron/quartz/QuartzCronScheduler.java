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
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.annotation.Nullable;
import javax.inject.Inject;

import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.AbstractIdleService;
import com.twitter.common.base.Function;
import com.twitter.common.stats.Stats;

import org.apache.aurora.scheduler.cron.CronException;
import org.apache.aurora.scheduler.cron.CronScheduler;
import org.quartz.CronExpression;
import org.quartz.CronScheduleBuilder;
import org.quartz.CronTrigger;
import org.quartz.Job;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.quartz.spi.JobFactory;
import org.quartz.spi.TriggerFiredBundle;

import static com.google.common.base.Preconditions.checkNotNull;

class QuartzCronScheduler extends AbstractIdleService implements CronScheduler {
  private static final Logger LOG = Logger.getLogger(QuartzCronScheduler.class.getName());
  private static final String QUARTZ_GROUP_NAME = "aurora-cron";
  private static org.quartz.JobKey getQuartzJobKey(String cronJobId) {
    return org.quartz.JobKey.jobKey(cronJobId, QUARTZ_GROUP_NAME);
  }

  private final AtomicInteger startedFlag = Stats.exportInt("quartz_scheduler_running");

  private final AtomicLong cronJobIdGenerator = new AtomicLong();
  private final ConcurrentMap<String, Runnable> cronJobIdToTask = Maps.newConcurrentMap();
  {
    Stats.exportSize("quartz_scheduler_cron_jobs_scheduled", cronJobIdToTask);
  }
  private final Job executeFromIdMap = new Job() {
    @Override public void execute(JobExecutionContext context) throws JobExecutionException {
      cronJobIdToTask.get(context.getJobDetail().getKey().getName()).run();
    }
  };
  private final JobFactory executeFromIdMapFactory = new JobFactory() {
    @Override public Job newJob(TriggerFiredBundle unused1, Scheduler unused2) {
      return executeFromIdMap;
    }
  };

  private final Scheduler scheduler;

  @Inject
  QuartzCronScheduler(Scheduler scheduler) {
    this.scheduler = checkNotNull(scheduler);
  }

  @Override
  protected void startUp() throws Exception {
    LOG.info("Starting Quartz cron scheduler.");
    scheduler.setJobFactory(executeFromIdMapFactory);
    scheduler.start();
    startedFlag.set(1);
  }

  @Override
  protected void shutDown() throws Exception {
    LOG.info("Shutting down Quartz cron scheduler.");
    scheduler.shutdown();
    startedFlag.set(0);
  }

  @Override
  public String schedule(String schedule, Runnable task) throws CronException {
    checkNotNull(schedule);
    checkNotNull(task);

    CronExpression cronExpression;
    try {
      cronExpression = new CronExpression(schedule);
    } catch (ParseException e) {
      throw new CronException(
          String.format("Invalid cron expression %s: %s.", schedule, e.getMessage()), e);
    }

    String cronJobId = Long.toString(cronJobIdGenerator.incrementAndGet());
    cronJobIdToTask.put(cronJobId, task);

    Trigger trigger = TriggerBuilder.newTrigger()
        .withIdentity(cronJobId, QUARTZ_GROUP_NAME)
        .withSchedule(CronScheduleBuilder.cronSchedule(cronExpression))
        .build();

    JobDetail jobDetail = JobBuilder.newJob(executeFromIdMap.getClass())
        .withIdentity(getQuartzJobKey(cronJobId))
        .build();

    try {
      scheduler.scheduleJob(jobDetail, trigger);
      return cronJobId;
    } catch (SchedulerException e) {
      LOG.log(Level.SEVERE, "Failed to schedule cron job " + cronJobId + ": " + e, e);
      throw new CronException(e);
    }
  }

  @Override
  public void deschedule(String cronJobId) {
    checkNotNull(cronJobId);

    cronJobIdToTask.remove(cronJobId);
    try {
      scheduler.deleteJob(getQuartzJobKey(cronJobId));
    } catch (SchedulerException e) {
      LOG.log(Level.SEVERE, "Couldn't delete job " + cronJobId + " from Quartz: " + e, e);
      throw Throwables.propagate(e);
    }
  }

  @Override
  public Optional<String> getSchedule(String cronJobId) throws IllegalStateException {
    checkNotNull(cronJobId);

    if (!cronJobIdToTask.containsKey(cronJobId)) {
      return Optional.absent();
    }

    try {
      return FluentIterable.from(scheduler.getTriggersOfJob(getQuartzJobKey(cronJobId)))
          .filter(CronTrigger.class)
          .transform(new Function<CronTrigger, String>() {
            @Override public String apply(CronTrigger trigger) {
              return trigger.getCronExpression();
            }
          })
          .first();
    } catch (SchedulerException e) {
      LOG.log(Level.SEVERE, "Error reading job " + cronJobId + " from Quartz: " + e, e);
      return Optional.absent();
    }
  }

  @Override
  public boolean isValidSchedule(@Nullable String schedule) {
    return CronExpression.isValidExpression(schedule);
  }
}
