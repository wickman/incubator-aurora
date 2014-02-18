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
package org.apache.aurora.scheduler.async;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableSet;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.common.testing.easymock.EasyMockTest;
import com.twitter.common.util.Clock;
import com.twitter.common.util.testing.FakeClock;

import org.apache.aurora.gen.AssignedTask;
import org.apache.aurora.gen.Identity;
import org.apache.aurora.gen.ScheduledTask;
import org.apache.aurora.gen.TaskConfig;
import org.apache.aurora.scheduler.async.TaskScheduler.TaskSchedulerImpl;
import org.apache.aurora.scheduler.base.Query;
import org.apache.aurora.scheduler.base.Tasks;
import org.apache.aurora.scheduler.events.EventSink;
import org.apache.aurora.scheduler.events.PubsubEvent.TaskStateChange;
import org.apache.aurora.scheduler.filter.CachedJobState;
import org.apache.aurora.scheduler.state.PubsubTestUtil;
import org.apache.aurora.scheduler.state.StateManager;
import org.apache.aurora.scheduler.state.TaskAssigner;
import org.apache.aurora.scheduler.storage.Storage;
import org.apache.aurora.scheduler.storage.Storage.MutableStoreProvider;
import org.apache.aurora.scheduler.storage.Storage.MutateWork;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.apache.aurora.scheduler.storage.mem.MemStorage;
import org.apache.aurora.scheduler.storage.testing.StorageTestUtil;
import org.apache.mesos.Protos.Offer;
import org.apache.mesos.Protos.TaskInfo;
import org.easymock.Capture;
import org.junit.Before;
import org.junit.Test;

import static org.apache.aurora.gen.ScheduleStatus.PENDING;
import static org.apache.aurora.gen.ScheduleStatus.THROTTLED;
import static org.apache.aurora.scheduler.async.TaskScheduler.TaskSchedulerResult.SUCCESS;
import static org.apache.aurora.scheduler.async.TaskScheduler.TaskSchedulerResult.TRY_AGAIN;
import static org.easymock.EasyMock.capture;
import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertEquals;

public class TaskSchedulerImplTest extends EasyMockTest {

  private static final IScheduledTask TASK_A = makeTask("a");
  private static final IScheduledTask TASK_B = makeTask("b");
  private static final Offer OFFER = Offers.makeOffer("OFFER_A", "HOST_A");

  private static final CachedJobState EMPTY_JOB =
      new CachedJobState(Suppliers.ofInstance(ImmutableSet.<IScheduledTask>of()));

  private StorageTestUtil storageUtil;
  private StateManager stateManager;
  private TaskAssigner assigner;
  private OfferQueue offerQueue;
  private TaskScheduler scheduler;
  private FakeClock clock;
  private Preemptor preemptor;
  private Amount<Long, Time> reservationDuration;
  private Amount<Long, Time> halfReservationDuration;
  private EventSink eventSink;

  @Before
  public void setUp() throws Exception {
    storageUtil = new StorageTestUtil(this);
    stateManager = createMock(StateManager.class);
    assigner = createMock(TaskAssigner.class);
    offerQueue = createMock(OfferQueue.class);
    reservationDuration = Amount.of(2L, Time.MINUTES);
    halfReservationDuration = Amount.of(1L, Time.MINUTES);
    clock = new FakeClock();
    clock.setNowMillis(0);
    preemptor = createMock(Preemptor.class);

    Injector injector = getInjector(storageUtil.storage);
    scheduler = injector.getInstance(TaskScheduler.class);
    eventSink = PubsubTestUtil.startPubsub(injector);
  }

  private Injector getInjector(final Storage storageImpl) {
    return Guice.createInjector(new AbstractModule() {
      @Override
      protected void configure() {
        PubsubTestUtil.installPubsub(binder());
        bind(AsyncModule.PREEMPTOR_KEY).toInstance(preemptor);
        AsyncModule.bindTaskScheduler(binder(), AsyncModule.PREEMPTOR_KEY, reservationDuration);
        bind(OfferQueue.class).toInstance(offerQueue);
        bind(StateManager.class).toInstance(stateManager);
        bind(TaskAssigner.class).toInstance(assigner);
        bind(Clock.class).toInstance(clock);
        bind(Storage.class).toInstance(storageImpl);
      }
    });
  }

  private void expectTaskStillPendingQuery(IScheduledTask task) {
    storageUtil.expectTaskFetch(
        Query.taskScoped(Tasks.id(task)).byStatus(PENDING),
        ImmutableSet.of(task));
  }

  private void expectAssigned(IScheduledTask task) {
    expect(assigner.maybeAssign(OFFER, task, EMPTY_JOB))
        .andReturn(Optional.of(TaskInfo.getDefaultInstance()));
  }

  @Test
  public void testReservationsDeniesTasksForTimePeriod() throws Exception {
    storageUtil.expectOperations();

    expectTaskStillPendingQuery(TASK_A);
    expectActiveJobFetch(TASK_A);
    expectLaunchAttempt(false);
    // Reserve "a" with offerA
    expect(preemptor.findPreemptionSlotFor("a", EMPTY_JOB))
        .andReturn(Optional.of(OFFER.getSlaveId().getValue()));

    expectTaskStillPendingQuery(TASK_B);
    expectActiveJobFetch(TASK_B);
    Capture<Function<Offer, Optional<TaskInfo>>> firstAssignment = expectLaunchAttempt(false);
    expect(preemptor.findPreemptionSlotFor("b", EMPTY_JOB)).andReturn(Optional.<String>absent());

    expectTaskStillPendingQuery(TASK_B);
    expectActiveJobFetch(TASK_B);
    Capture<Function<Offer, Optional<TaskInfo>>> secondAssignment = expectLaunchAttempt(true);
    expectAssigned(TASK_B);

    control.replay();

    assertEquals(TRY_AGAIN, scheduler.schedule("a"));
    assertEquals(TRY_AGAIN, scheduler.schedule("b"));

    assertEquals(Optional.<TaskInfo>absent(), firstAssignment.getValue().apply(OFFER));

    clock.advance(reservationDuration);

    assertEquals(SUCCESS, scheduler.schedule("b"));

    assertEquals(true, secondAssignment.getValue().apply(OFFER).isPresent());
  }

  @Test
  public void testReservationsExpireAfterAccepted() throws Exception {
    storageUtil.expectOperations();

    expectTaskStillPendingQuery(TASK_A);
    expectActiveJobFetch(TASK_A);
    expectLaunchAttempt(false);
    // Reserve "a" with offerA
    expect(preemptor.findPreemptionSlotFor("a", EMPTY_JOB))
        .andReturn(Optional.of(OFFER.getSlaveId().getValue()));

    expectTaskStillPendingQuery(TASK_A);
    expectActiveJobFetch(TASK_A);
    Capture<Function<Offer, Optional<TaskInfo>>> firstAssignment = expectLaunchAttempt(true);
    expectAssigned(TASK_A);

    expectTaskStillPendingQuery(TASK_B);
    expectActiveJobFetch(TASK_B);

    Capture<Function<Offer, Optional<TaskInfo>>> secondAssignment = expectLaunchAttempt(true);

    expect(assigner.maybeAssign(OFFER, TASK_B, EMPTY_JOB))
        .andReturn(Optional.of(TaskInfo.getDefaultInstance()));

    control.replay();
    assertEquals(TRY_AGAIN, scheduler.schedule("a"));
    assertEquals(SUCCESS, scheduler.schedule("a"));
    firstAssignment.getValue().apply(OFFER);
    eventSink.post(TaskStateChange.transition(TASK_A, PENDING));
    clock.advance(halfReservationDuration);
    assertEquals(SUCCESS, scheduler.schedule("b"));
    secondAssignment.getValue().apply(OFFER);
  }

  @Test
  public void testReservationsAcceptsWithInTimePeriod() throws Exception {
    storageUtil.expectOperations();
    expectTaskStillPendingQuery(TASK_A);
    expectActiveJobFetch(TASK_A);
    expectLaunchAttempt(false);
    // Reserve "a" with offerA
    expect(preemptor.findPreemptionSlotFor("a", EMPTY_JOB))
        .andReturn(Optional.of(OFFER.getSlaveId().getValue()));

    expectTaskStillPendingQuery(TASK_A);
    expectActiveJobFetch(TASK_A);
    Capture<Function<Offer, Optional<TaskInfo>>> firstAssignment = expectLaunchAttempt(true);
    expectAssigned(TASK_A);

    control.replay();
    assertEquals(TRY_AGAIN, scheduler.schedule("a"));
    clock.advance(halfReservationDuration);
    assertEquals(SUCCESS, scheduler.schedule("a"));

    firstAssignment.getValue().apply(OFFER);
  }

  @Test
  public void testReservationsCancellation() throws Exception {
    storageUtil.expectOperations();

    expectTaskStillPendingQuery(TASK_A);
    expectActiveJobFetch(TASK_A);
    expectLaunchAttempt(false);

    // Reserve "a" with offerA
    expect(preemptor.findPreemptionSlotFor("a", EMPTY_JOB))
        .andReturn(Optional.of(OFFER.getSlaveId().getValue()));

    expectTaskStillPendingQuery(TASK_B);
    expectActiveJobFetch(TASK_B);
    Capture<Function<Offer, Optional<TaskInfo>>> assignment = expectLaunchAttempt(true);
    expectAssigned(TASK_B);

    control.replay();
    assertEquals(TRY_AGAIN, scheduler.schedule("a"));
    clock.advance(halfReservationDuration);
    // Task is killed by user before it is scheduled
    eventSink.post(TaskStateChange.transition(TASK_A, PENDING));
    assertEquals(SUCCESS, scheduler.schedule("b"));
    assignment.getValue().apply(OFFER);
  }

  @Test
  public void testReservationsExpire() throws Exception {
    storageUtil.expectOperations();

    expectTaskStillPendingQuery(TASK_B);
    expectActiveJobFetch(TASK_B);
    expectLaunchAttempt(false);
    // Reserve "b" with offer1
    expect(preemptor.findPreemptionSlotFor("b", EMPTY_JOB))
        .andReturn(Optional.of(OFFER.getSlaveId().getValue()));

    expectTaskStillPendingQuery(TASK_A);
    expectActiveJobFetch(TASK_A);
    Capture<Function<Offer, Optional<TaskInfo>>> firstAssignment = expectLaunchAttempt(true);
    expectAssigned(TASK_A);

    control.replay();
    assertEquals(TRY_AGAIN, scheduler.schedule("b"));
    // We don't act on the reservation made by b because we want to see timeout behaviour.
    clock.advance(reservationDuration);
    assertEquals(SUCCESS, scheduler.schedule("a"));
    firstAssignment.getValue().apply(OFFER);
  }

  @Test
  public void testIgnoresThrottledTasks() throws Exception {
    // Ensures that tasks in THROTTLED state are not considered part of the active job state passed
    // to the assigner function.

    Storage memStorage = MemStorage.newEmptyStorage();

    Injector injector = getInjector(memStorage);
    scheduler = injector.getInstance(TaskScheduler.class);
    eventSink = PubsubTestUtil.startPubsub(injector);

    ScheduledTask builder = TASK_A.newBuilder();
    final IScheduledTask taskA = IScheduledTask.build(builder.setStatus(PENDING));
    builder.getAssignedTask().setTaskId("b");
    final IScheduledTask taskB = IScheduledTask.build(builder.setStatus(THROTTLED));

    memStorage.write(new MutateWork.NoResult.Quiet() {
      @Override
      protected void execute(MutableStoreProvider store) {
        store.getUnsafeTaskStore().saveTasks(ImmutableSet.of(taskA, taskB));
      }
    });

    Capture<Function<Offer, Optional<TaskInfo>>> assignment = expectLaunchAttempt(true);
    expect(assigner.maybeAssign(OFFER, taskA, EMPTY_JOB))
        .andReturn(Optional.of(TaskInfo.getDefaultInstance()));

    control.replay();

    assertEquals(SUCCESS, scheduler.schedule(Tasks.id(taskA)));
    assignment.getValue().apply(OFFER);
  }

  private static IScheduledTask makeTask(String taskId) {
    return IScheduledTask.build(new ScheduledTask()
        .setAssignedTask(new AssignedTask()
            .setInstanceId(0)
            .setTaskId(taskId)
            .setTask(new TaskConfig()
                .setJobName("job-" + taskId)
                .setOwner(new Identity().setRole("role-" + taskId).setUser("user-" + taskId))
                .setEnvironment("env-" + taskId))));
  }

  private Capture<Function<Offer, Optional<TaskInfo>>> expectLaunchAttempt(boolean taskLaunched)
      throws OfferQueue.LaunchException {
        Capture<Function<Offer, Optional<TaskInfo>>> assignment = createCapture();
        expect(offerQueue.launchFirst(capture(assignment))).andReturn(taskLaunched);
        return assignment;
  }

  private void expectActiveJobFetch(IScheduledTask taskInJob) {
    storageUtil.expectTaskFetch(
        TaskSchedulerImpl.activeJobStateQuery(Tasks.SCHEDULED_TO_JOB_KEY.apply(taskInJob)),
        ImmutableSet.<IScheduledTask>of());
  }
}
