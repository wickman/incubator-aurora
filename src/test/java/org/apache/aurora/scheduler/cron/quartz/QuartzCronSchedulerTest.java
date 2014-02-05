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

import com.google.common.collect.ImmutableList;
import com.twitter.common.testing.easymock.EasyMockTest;

import org.apache.aurora.scheduler.cron.CronException;
import org.apache.aurora.scheduler.cron.CronScheduler;
import org.easymock.Capture;
import org.easymock.IAnswer;
import org.junit.Before;
import org.junit.Test;
import org.quartz.JobDetail;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.Trigger;
import org.quartz.spi.JobFactory;

import static org.easymock.EasyMock.capture;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.isA;
import static org.junit.Assert.assertEquals;

public class QuartzCronSchedulerTest extends EasyMockTest {
  private Scheduler scheduler;

  private CronScheduler cronScheduler;

  @Before
  public void setUp() {
    scheduler = createMock(Scheduler.class);

    cronScheduler = new QuartzCronScheduler(scheduler);
  }

  @Test
  public void testLifecycle() throws Exception {
    scheduler.setJobFactory(isA(JobFactory.class));
    scheduler.start();
    scheduler.shutdown();

    control.replay();

    cronScheduler.startAsync().awaitRunning();
    cronScheduler.stopAsync().awaitTerminated();
  }

  @Test(expected = CronException.class)
  public void testScheduleFailed() throws Exception {
    expect(scheduler.scheduleJob(isA(JobDetail.class), isA(Trigger.class)))
        .andThrow(new SchedulerException("Test fail."));

    control.replay();

    cronScheduler.schedule("* * * * * ?", new Runnable() {
      @Override public void run() {
       // Not reached.
      }
   });
  }

  @Test
  public void testJobLifecycle() throws Exception {
    String schedule = "* */3 */2 * * ?";

    Capture<JobDetail> scheduledJobDetail = createCapture();
    final Capture<Trigger> cronTrigger = createCapture();
    Capture<JobKey> descheduledJobKey = createCapture();
    expect(scheduler.scheduleJob(capture(scheduledJobDetail), capture(cronTrigger)))
        .andReturn(null);

    expect(scheduler.getTriggersOfJob(isA(JobKey.class)));
    expectLastCall().andAnswer(new IAnswer<Object>() {
      @Override public Object answer() throws Throwable {
        return ImmutableList.of(cronTrigger.getValue());
      }
    });
    expect(scheduler.deleteJob(capture(descheduledJobKey))).andReturn(true);

    control.replay();

    String cronJobId = cronScheduler.schedule(schedule, new Runnable() {
      @Override public void run() {
        // Not reached.
      }
    });

    assertEquals(schedule, cronScheduler.getSchedule(cronJobId).orNull());

    cronScheduler.deschedule(cronJobId);

    assertEquals(scheduledJobDetail.getValue().getKey(), descheduledJobKey.getValue());
    assertEquals(null, cronScheduler.getSchedule(cronJobId).orNull());
  }
}
