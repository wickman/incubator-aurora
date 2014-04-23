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
package org.apache.aurora.scheduler.state;

import java.util.Map;
import java.util.Set;

import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import org.apache.aurora.gen.AssignedTask;
import org.apache.aurora.gen.Identity;
import org.apache.aurora.gen.ScheduleStatus;
import org.apache.aurora.gen.ScheduledTask;
import org.apache.aurora.gen.TaskConfig;
import org.apache.aurora.scheduler.base.Tasks;
import org.apache.aurora.scheduler.state.SideEffect.Action;
import org.apache.aurora.scheduler.state.TaskStateMachine.TaskState;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.junit.Before;
import org.junit.Test;

import static org.apache.aurora.scheduler.state.TaskStateMachine.TaskState.ASSIGNED;
import static org.apache.aurora.scheduler.state.TaskStateMachine.TaskState.DELETED;
import static org.apache.aurora.scheduler.state.TaskStateMachine.TaskState.DRAINING;
import static org.apache.aurora.scheduler.state.TaskStateMachine.TaskState.FAILED;
import static org.apache.aurora.scheduler.state.TaskStateMachine.TaskState.FINISHED;
import static org.apache.aurora.scheduler.state.TaskStateMachine.TaskState.INIT;
import static org.apache.aurora.scheduler.state.TaskStateMachine.TaskState.KILLED;
import static org.apache.aurora.scheduler.state.TaskStateMachine.TaskState.KILLING;
import static org.apache.aurora.scheduler.state.TaskStateMachine.TaskState.LOST;
import static org.apache.aurora.scheduler.state.TaskStateMachine.TaskState.PENDING;
import static org.apache.aurora.scheduler.state.TaskStateMachine.TaskState.PREEMPTING;
import static org.apache.aurora.scheduler.state.TaskStateMachine.TaskState.RESTARTING;
import static org.apache.aurora.scheduler.state.TaskStateMachine.TaskState.RUNNING;
import static org.apache.aurora.scheduler.state.TaskStateMachine.TaskState.SANDBOX_DELETED;
import static org.apache.aurora.scheduler.state.TaskStateMachine.TaskState.STARTING;
import static org.apache.aurora.scheduler.state.TaskStateMachine.TaskState.THROTTLED;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

// TODO(wfarner): At this rate, it's probably best to exhaustively cover this class with a matrix
// from every state to every state.
public class TaskStateMachineTest {

  private TaskStateMachine stateMachine;

  @Before
  public void setUp() {
    stateMachine = makeStateMachine(makeTask(false));
  }

  private TaskStateMachine makeStateMachine(ScheduledTask builder) {
    return new TaskStateMachine(IScheduledTask.build(builder));
  }

  @Test
  public void testSimpleTransition() {
    expectUpdateStateOnTransitionTo(PENDING, ASSIGNED, STARTING, RUNNING, FINISHED);
    legalTransition(SANDBOX_DELETED, Action.SAVE_STATE);
  }

  @Test
  public void testServiceRescheduled() {
    stateMachine = makeStateMachine(makeTask(true));
    expectUpdateStateOnTransitionTo(PENDING, ASSIGNED, STARTING, RUNNING);
    legalTransition(FINISHED, Action.SAVE_STATE, Action.RESCHEDULE);
  }

  @Test
  public void testPostTerminalTransitionDenied() {
    Set<ScheduleStatus> terminalStates =
        Sets.difference(Tasks.TERMINAL_STATES, ImmutableSet.of(ScheduleStatus.SANDBOX_DELETED));

    for (ScheduleStatus endState : terminalStates) {
      stateMachine = makeStateMachine(makeTask(false));
      Set<SideEffect.Action> finalActions = Sets.newHashSet(Action.SAVE_STATE);

      switch (endState) {
        case FAILED:
          finalActions.add(Action.INCREMENT_FAILURES);
          break;

        case FINISHED:
          break;

        case KILLED:
        case LOST:
          finalActions.add(Action.RESCHEDULE);
          break;

        case KILLING:
          finalActions.add(Action.KILL);
          break;

        default:
          fail("Unknown state " + endState);
      }

      expectUpdateStateOnTransitionTo(PENDING, ASSIGNED, STARTING, RUNNING);
      legalTransition(TaskState.valueOf(endState.name()), finalActions);

      for (ScheduleStatus badTransition : terminalStates) {
        illegalTransition(TaskState.valueOf(badTransition.name()));
      }
    }
  }

  @Test
  public void testTerminalToSandboxDeleted() {
    Set<ScheduleStatus> terminalStates =
        Sets.difference(Tasks.TERMINAL_STATES, ImmutableSet.of(ScheduleStatus.SANDBOX_DELETED));
    System.out.print(terminalStates);
    for (ScheduleStatus endState : terminalStates) {
      stateMachine = makeStateMachine(makeTask(false));
      Set<SideEffect.Action> finalActions = Sets.newHashSet(Action.SAVE_STATE);

      switch (endState) {
        case FAILED:
          finalActions.add(Action.INCREMENT_FAILURES);
          break;

        case FINISHED:
          break;

        case KILLED:
        case LOST:
          finalActions.add(Action.RESCHEDULE);
          break;

        case KILLING:
          finalActions.add(Action.KILL);
          break;

        default:
          fail("Unknown state " + endState);
      }

      expectUpdateStateOnTransitionTo(
          PENDING,
          ASSIGNED,
          STARTING,
          RUNNING);
      legalTransition(TaskState.valueOf(endState.name()), finalActions);
      legalTransition(SANDBOX_DELETED, Action.SAVE_STATE);
    }
  }

  @Test
  public void testTerminalToDeleted() {
    Set<ScheduleStatus> terminalStates =
        Sets.difference(Tasks.TERMINAL_STATES, ImmutableSet.of(ScheduleStatus.SANDBOX_DELETED));
    System.out.print(terminalStates);
    for (ScheduleStatus endState : terminalStates) {
      stateMachine = makeStateMachine(makeTask(false));
      Set<SideEffect.Action> finalActions = Sets.newHashSet(Action.SAVE_STATE);

      switch (endState) {
        case FAILED:
          finalActions.add(Action.INCREMENT_FAILURES);
          break;

        case FINISHED:
          break;

        case KILLED:
        case LOST:
          finalActions.add(Action.RESCHEDULE);
          break;

        case KILLING:
          finalActions.add(Action.KILL);
          break;

        default:
          fail("Unknown state " + endState);
      }

      expectUpdateStateOnTransitionTo(
          PENDING,
          ASSIGNED,
          STARTING,
          RUNNING);
      legalTransition(TaskState.valueOf(endState.name()), finalActions);
      legalTransition(DELETED, Action.DELETE);
    }
  }

  @Test
  public void testSandboxDeletedToDeleted() {
    stateMachine = makeStateMachine(makeTask(false));
    expectUpdateStateOnTransitionTo(
        PENDING,
        ASSIGNED,
        STARTING,
        RUNNING,
        FINISHED,
        SANDBOX_DELETED);
    legalTransition(DELETED, Action.DELETE);
  }

  @Test
  public void testUnknownTask() {
    stateMachine = new TaskStateMachine("id");

    illegalTransition(RUNNING, Action.KILL);
  }

  @Test
  public void testLostTask() {
    expectUpdateStateOnTransitionTo(PENDING, ASSIGNED, STARTING, RUNNING);
    legalTransition(LOST, Action.SAVE_STATE, Action.RESCHEDULE);
  }

  @Test
  public void testKilledPending() {
    expectUpdateStateOnTransitionTo(PENDING);
    legalTransition(KILLING, Action.DELETE);
  }

  @Test
  public void testMissingStartingRescheduledImmediately() {
    expectUpdateStateOnTransitionTo(PENDING, ASSIGNED, STARTING);
    illegalTransition(SANDBOX_DELETED,
        ImmutableSet.of(new SideEffect(Action.STATE_CHANGE, Optional.of(LOST.getStatus().get()))));
  }

  @Test
  public void testMissingRunningRescheduledImmediately() {
    expectUpdateStateOnTransitionTo(PENDING, ASSIGNED, STARTING, RUNNING);
    illegalTransition(SANDBOX_DELETED,
        ImmutableSet.of(new SideEffect(Action.STATE_CHANGE, Optional.of(LOST.getStatus().get()))));
  }

  @Test
  public void testRestartedTask() {
    expectUpdateStateOnTransitionTo(PENDING, ASSIGNED, STARTING, RUNNING);
    legalTransition(RESTARTING, Action.SAVE_STATE, Action.KILL);
    legalTransition(FINISHED, Action.SAVE_STATE, Action.RESCHEDULE);
  }

  @Test
  public void testRogueRestartedTask() {
    expectUpdateStateOnTransitionTo(PENDING, ASSIGNED, STARTING, RUNNING);
    legalTransition(RESTARTING, Action.SAVE_STATE, Action.KILL);
    illegalTransition(RUNNING, Action.KILL);
  }

  @Test
  public void testPendingRestartedTask() {
    expectUpdateStateOnTransitionTo(PENDING);
    // PENDING -> RESTARTING should not be allowed.
    illegalTransition(RESTARTING);
  }

  @Test
  public void testDrainedTask() {
    expectUpdateStateOnTransitionTo(PENDING, ASSIGNED, STARTING, RUNNING);
    legalTransition(DRAINING, Action.SAVE_STATE, Action.KILL);
    legalTransition(FINISHED, Action.SAVE_STATE, Action.RESCHEDULE);
  }

  @Test
  public void testRogueDrainedTask() {
    expectUpdateStateOnTransitionTo(PENDING, ASSIGNED, STARTING, RUNNING);
    legalTransition(DRAINING, Action.SAVE_STATE, Action.KILL);
    illegalTransition(RUNNING, Action.KILL);
  }

  @Test
  public void testPendingDrainedTask() {
    expectUpdateStateOnTransitionTo(PENDING);
    // PENDING -> DRAINING should not be allowed.
    illegalTransition(DRAINING);
  }

  @Test
  public void testAllowsSkipStartingAndRunning() {
    expectUpdateStateOnTransitionTo(PENDING, ASSIGNED, FINISHED);
  }

  @Test
  public void testAllowsSkipRunning() {
    expectUpdateStateOnTransitionTo(PENDING, ASSIGNED, STARTING, FINISHED);
  }

  @Test
  public void testHonorsMaxFailures() {
    ScheduledTask task = makeTask(false);
    task.getAssignedTask().getTask().setMaxTaskFailures(10);
    task.setFailureCount(8);
    stateMachine = makeStateMachine(task);
    expectUpdateStateOnTransitionTo(PENDING, ASSIGNED, STARTING, RUNNING);
    legalTransition(FAILED, Action.SAVE_STATE, Action.RESCHEDULE, Action.INCREMENT_FAILURES);

    ScheduledTask rescheduled = task.deepCopy();
    rescheduled.setFailureCount(9);
    stateMachine = makeStateMachine(rescheduled);
    expectUpdateStateOnTransitionTo(PENDING, ASSIGNED, STARTING, RUNNING);
    legalTransition(FAILED, Action.SAVE_STATE, Action.INCREMENT_FAILURES);
  }

  @Test
  public void testHonorsUnlimitedFailures() {
    ScheduledTask task = makeTask(false);
    task.getAssignedTask().getTask().setMaxTaskFailures(-1);
    task.setFailureCount(1000);
    stateMachine = makeStateMachine(task);

    expectUpdateStateOnTransitionTo(PENDING, ASSIGNED, STARTING, RUNNING);
    legalTransition(FAILED, Action.SAVE_STATE, Action.RESCHEDULE, Action.INCREMENT_FAILURES);
  }

  @Test
  public void testKillingRequest() {
    expectUpdateStateOnTransitionTo(PENDING, ASSIGNED, STARTING, RUNNING);
    legalTransition(KILLING, Action.KILL, Action.SAVE_STATE);
    expectUpdateStateOnTransitionTo(KILLED);
  }

  @Test
  public void testThrottledTask() {
    expectUpdateStateOnTransitionTo(THROTTLED, PENDING);
  }

  private static final Function<Action, SideEffect> TO_SIDE_EFFECT =
      new Function<Action, SideEffect>() {
        @Override
        public SideEffect apply(Action action) {
          return new SideEffect(action, Optional.<ScheduleStatus>absent());
        }
      };

  private void legalTransition(TaskState state, SideEffect.Action... expectedActions) {
    legalTransition(state, ImmutableSet.copyOf(expectedActions));
  }

  private void legalTransition(TaskState state, Set<SideEffect.Action> expectedActions) {
    ScheduleStatus previousState = stateMachine.getPreviousState();
    TransitionResult result = stateMachine.updateState(state.getStatus());
    assertTrue("Transition to " + state + " was not successful", result.isSuccess());
    assertNotEquals(previousState, stateMachine.getPreviousState());
    assertEquals(
        FluentIterable.from(expectedActions).transform(TO_SIDE_EFFECT).toSet(),
        result.getSideEffects());
  }

  private void expectUpdateStateOnTransitionTo(TaskState... states) {
    for (TaskState status : states) {
      legalTransition(status, Action.SAVE_STATE);
    }
  }

  private void illegalTransition(TaskState state, SideEffect.Action... expectedActions) {
    illegalTransition(
        state,
        FluentIterable.from(
            ImmutableSet.copyOf(expectedActions)).transform(TO_SIDE_EFFECT).toSet());
  }

  private void illegalTransition(TaskState state, Set<SideEffect> sideEffects) {
    TransitionResult result = stateMachine.updateState(state.getStatus());
    assertFalse(result.isSuccess());
    assertEquals(sideEffects, result.getSideEffects());
  }

  private static ScheduledTask makeTask(boolean service) {
    return new ScheduledTask()
        .setStatus(INIT.getStatus().get())
        .setAssignedTask(
            new AssignedTask()
                .setTaskId("test")
                .setTask(
                    new TaskConfig()
                        .setOwner(new Identity().setRole("roleA"))
                        .setJobName("jobA")
                        .setIsService(service)));
  }

  private static final TransitionResult SAVE = new TransitionResult(
      true,
      ImmutableSet.of(new SideEffect(Action.SAVE_STATE, Optional.<ScheduleStatus>absent())));
  private static final TransitionResult SAVE_AND_KILL = new TransitionResult(
      true,
      ImmutableSet.of(
          new SideEffect(Action.SAVE_STATE, Optional.<ScheduleStatus>absent()),
          new SideEffect(Action.KILL, Optional.<ScheduleStatus>absent())));
  private static final TransitionResult SAVE_AND_RESCHEDULE = new TransitionResult(
      true,
      ImmutableSet.of(
          new SideEffect(Action.SAVE_STATE, Optional.<ScheduleStatus>absent()),
          new SideEffect(Action.RESCHEDULE, Optional.<ScheduleStatus>absent())));
  private static final TransitionResult SAVE_KILL_AND_RESCHEDULE = new TransitionResult(
      true,
      ImmutableSet.of(
          new SideEffect(Action.SAVE_STATE, Optional.<ScheduleStatus>absent()),
          new SideEffect(Action.KILL, Optional.<ScheduleStatus>absent()),
          new SideEffect(Action.RESCHEDULE, Optional.<ScheduleStatus>absent())));
  private static final TransitionResult ILLEGAL_KILL = new TransitionResult(
      false,
      ImmutableSet.of(new SideEffect(Action.KILL, Optional.<ScheduleStatus>absent())));
  private static final TransitionResult RECORD_FAILURE = new TransitionResult(
      true,
      ImmutableSet.of(
          new SideEffect(Action.SAVE_STATE, Optional.<ScheduleStatus>absent()),
          new SideEffect(Action.INCREMENT_FAILURES, Optional.<ScheduleStatus>absent())));
  private static final TransitionResult DELETE_TASK = new TransitionResult(
      true,
      ImmutableSet.of(new SideEffect(Action.DELETE, Optional.<ScheduleStatus>absent())));
  private static final TransitionResult MARK_LOST = new TransitionResult(
      false,
      ImmutableSet.of(new SideEffect(Action.STATE_CHANGE, Optional.of(LOST.getStatus().get()))));

  private static final class TestCase {
    private final boolean taskPresent;
    private final TaskState from;
    private final TaskState to;

    private TestCase(boolean taskPresent, TaskState from, TaskState to) {
      this.taskPresent = taskPresent;
      this.from = from;
      this.to = to;
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(taskPresent, from, to);
    }

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof TestCase)) {
        return false;
      }

      TestCase other = (TestCase) o;
      return (taskPresent == other.taskPresent)
          && (from == other.from)
          && (to == other.to);
    }

    @Override
    public String toString() {
      return Objects.toStringHelper(this)
          .add("taskPresent", taskPresent)
          .add("from", from)
          .add("to", to)
          .toString();
    }
  }

  private static final Map<TestCase, TransitionResult> EXPECTATIONS =
      ImmutableMap.<TestCase, TransitionResult>builder()
          .put(new TestCase(true, INIT, THROTTLED), SAVE)
          .put(new TestCase(true, INIT, PENDING), SAVE)
          .put(new TestCase(false, INIT, ASSIGNED), ILLEGAL_KILL)
          .put(new TestCase(false, INIT, STARTING), ILLEGAL_KILL)
          .put(new TestCase(false, INIT, RUNNING), ILLEGAL_KILL)
          .put(new TestCase(true, THROTTLED, PENDING), SAVE)
          .put(new TestCase(true, THROTTLED, KILLING), DELETE_TASK)
          .put(new TestCase(false, THROTTLED, ASSIGNED), ILLEGAL_KILL)
          .put(new TestCase(false, THROTTLED, STARTING), ILLEGAL_KILL)
          .put(new TestCase(false, THROTTLED, RUNNING), ILLEGAL_KILL)
          .put(new TestCase(true, PENDING, ASSIGNED), SAVE)
          .put(new TestCase(false, PENDING, ASSIGNED), ILLEGAL_KILL)
          .put(new TestCase(false, PENDING, STARTING), ILLEGAL_KILL)
          .put(new TestCase(false, PENDING, RUNNING), ILLEGAL_KILL)
          .put(new TestCase(true, PENDING, KILLING), DELETE_TASK)
          .put(new TestCase(false, ASSIGNED, ASSIGNED), ILLEGAL_KILL)
          .put(new TestCase(true, ASSIGNED, STARTING), SAVE)
          .put(new TestCase(false, ASSIGNED, STARTING), ILLEGAL_KILL)
          .put(new TestCase(true, ASSIGNED, RUNNING), SAVE)
          .put(new TestCase(false, ASSIGNED, RUNNING), ILLEGAL_KILL)
          .put(new TestCase(true, ASSIGNED, FINISHED), SAVE)
          .put(new TestCase(true, ASSIGNED, PREEMPTING), SAVE_AND_KILL)
          .put(new TestCase(true, ASSIGNED, RESTARTING), SAVE_AND_KILL)
          .put(new TestCase(true, ASSIGNED, DRAINING), SAVE_AND_KILL)
          .put(new TestCase(true, ASSIGNED, FAILED), RECORD_FAILURE)
          .put(new TestCase(true, ASSIGNED, KILLED), SAVE_AND_RESCHEDULE)
          .put(new TestCase(true, ASSIGNED, KILLING), SAVE_AND_KILL)
          .put(new TestCase(true, ASSIGNED, LOST), SAVE_KILL_AND_RESCHEDULE)
          .put(new TestCase(false, STARTING, ASSIGNED), ILLEGAL_KILL)
          .put(new TestCase(false, STARTING, STARTING), ILLEGAL_KILL)
          .put(new TestCase(true, STARTING, RUNNING), SAVE)
          .put(new TestCase(false, STARTING, RUNNING), ILLEGAL_KILL)
          .put(new TestCase(true, STARTING, FINISHED), SAVE)
          .put(new TestCase(true, STARTING, PREEMPTING), SAVE_AND_KILL)
          .put(new TestCase(true, STARTING, RESTARTING), SAVE_AND_KILL)
          .put(new TestCase(true, STARTING, DRAINING), SAVE_AND_KILL)
          .put(new TestCase(true, STARTING, FAILED), RECORD_FAILURE)
          .put(new TestCase(true, STARTING, KILLED), SAVE_AND_RESCHEDULE)
          .put(new TestCase(true, STARTING, KILLING), SAVE_AND_KILL)
          .put(new TestCase(true, STARTING, LOST), SAVE_AND_RESCHEDULE)
          .put(new TestCase(true, STARTING, SANDBOX_DELETED), MARK_LOST)
          .put(new TestCase(false, RUNNING, ASSIGNED), ILLEGAL_KILL)
          .put(new TestCase(false, RUNNING, STARTING), ILLEGAL_KILL)
          .put(new TestCase(false, RUNNING, RUNNING), ILLEGAL_KILL)
          .put(new TestCase(true, RUNNING, FINISHED), SAVE)
          .put(new TestCase(true, RUNNING, PREEMPTING), SAVE_AND_KILL)
          .put(new TestCase(true, RUNNING, RESTARTING), SAVE_AND_KILL)
          .put(new TestCase(true, RUNNING, DRAINING), SAVE_AND_KILL)
          .put(new TestCase(true, RUNNING, FAILED), RECORD_FAILURE)
          .put(new TestCase(true, RUNNING, KILLED), SAVE_AND_RESCHEDULE)
          .put(new TestCase(true, RUNNING, KILLING), SAVE_AND_KILL)
          .put(new TestCase(true, RUNNING, LOST), SAVE_AND_RESCHEDULE)
          .put(new TestCase(true, RUNNING, SANDBOX_DELETED), MARK_LOST)
          .put(new TestCase(true, FINISHED, ASSIGNED), ILLEGAL_KILL)
          .put(new TestCase(false, FINISHED, ASSIGNED), ILLEGAL_KILL)
          .put(new TestCase(true, FINISHED, STARTING), ILLEGAL_KILL)
          .put(new TestCase(false, FINISHED, STARTING), ILLEGAL_KILL)
          .put(new TestCase(true, FINISHED, RUNNING), ILLEGAL_KILL)
          .put(new TestCase(false, FINISHED, RUNNING), ILLEGAL_KILL)
          .put(new TestCase(true, FINISHED, SANDBOX_DELETED), SAVE)
          .put(new TestCase(true, FINISHED, DELETED), DELETE_TASK)
          .put(new TestCase(true, PREEMPTING, ASSIGNED), ILLEGAL_KILL)
          .put(new TestCase(false, PREEMPTING, ASSIGNED), ILLEGAL_KILL)
          .put(new TestCase(true, PREEMPTING, STARTING), ILLEGAL_KILL)
          .put(new TestCase(false, PREEMPTING, STARTING), ILLEGAL_KILL)
          .put(new TestCase(true, PREEMPTING, RUNNING), ILLEGAL_KILL)
          .put(new TestCase(false, PREEMPTING, RUNNING), ILLEGAL_KILL)
          .put(new TestCase(true, PREEMPTING, FINISHED), SAVE_AND_RESCHEDULE)
          .put(new TestCase(true, PREEMPTING, FAILED), SAVE_AND_RESCHEDULE)
          .put(new TestCase(true, PREEMPTING, KILLED), SAVE_AND_RESCHEDULE)
          .put(new TestCase(true, PREEMPTING, KILLING), SAVE)
          .put(new TestCase(true, PREEMPTING, LOST), SAVE_KILL_AND_RESCHEDULE)
          .put(new TestCase(true, PREEMPTING, SANDBOX_DELETED), MARK_LOST)
          .put(new TestCase(true, RESTARTING, ASSIGNED), ILLEGAL_KILL)
          .put(new TestCase(false, RESTARTING, ASSIGNED), ILLEGAL_KILL)
          .put(new TestCase(true, RESTARTING, STARTING), ILLEGAL_KILL)
          .put(new TestCase(false, RESTARTING, STARTING), ILLEGAL_KILL)
          .put(new TestCase(true, RESTARTING, RUNNING), ILLEGAL_KILL)
          .put(new TestCase(false, RESTARTING, RUNNING), ILLEGAL_KILL)
          .put(new TestCase(true, RESTARTING, FINISHED), SAVE_AND_RESCHEDULE)
          .put(new TestCase(true, RESTARTING, FAILED), SAVE_AND_RESCHEDULE)
          .put(new TestCase(true, RESTARTING, KILLED), SAVE_AND_RESCHEDULE)
          .put(new TestCase(true, RESTARTING, KILLING), SAVE)
          .put(new TestCase(true, RESTARTING, LOST), SAVE_KILL_AND_RESCHEDULE)
          .put(new TestCase(true, RESTARTING, SANDBOX_DELETED), MARK_LOST)
          .put(new TestCase(true, DRAINING, ASSIGNED), ILLEGAL_KILL)
          .put(new TestCase(false, DRAINING, ASSIGNED), ILLEGAL_KILL)
          .put(new TestCase(true, DRAINING, STARTING), ILLEGAL_KILL)
          .put(new TestCase(false, DRAINING, STARTING), ILLEGAL_KILL)
          .put(new TestCase(true, DRAINING, RUNNING), ILLEGAL_KILL)
          .put(new TestCase(false, DRAINING, RUNNING), ILLEGAL_KILL)
          .put(new TestCase(true, DRAINING, FINISHED), SAVE_AND_RESCHEDULE)
          .put(new TestCase(true, DRAINING, FAILED), SAVE_AND_RESCHEDULE)
          .put(new TestCase(true, DRAINING, KILLED), SAVE_AND_RESCHEDULE)
          .put(new TestCase(true, DRAINING, KILLING), SAVE)
          .put(new TestCase(true, DRAINING, LOST), SAVE_KILL_AND_RESCHEDULE)
          .put(new TestCase(true, DRAINING, SANDBOX_DELETED), MARK_LOST)
          .put(new TestCase(true, FAILED, ASSIGNED), ILLEGAL_KILL)
          .put(new TestCase(false, FAILED, ASSIGNED), ILLEGAL_KILL)
          .put(new TestCase(true, FAILED, STARTING), ILLEGAL_KILL)
          .put(new TestCase(false, FAILED, STARTING), ILLEGAL_KILL)
          .put(new TestCase(true, FAILED, RUNNING), ILLEGAL_KILL)
          .put(new TestCase(false, FAILED, RUNNING), ILLEGAL_KILL)
          .put(new TestCase(true, FAILED, SANDBOX_DELETED), SAVE)
          .put(new TestCase(true, FAILED, DELETED), DELETE_TASK)
          .put(new TestCase(true, KILLED, ASSIGNED), ILLEGAL_KILL)
          .put(new TestCase(false, KILLED, ASSIGNED), ILLEGAL_KILL)
          .put(new TestCase(true, KILLED, STARTING), ILLEGAL_KILL)
          .put(new TestCase(false, KILLED, STARTING), ILLEGAL_KILL)
          .put(new TestCase(true, KILLED, RUNNING), ILLEGAL_KILL)
          .put(new TestCase(false, KILLED, RUNNING), ILLEGAL_KILL)
          .put(new TestCase(true, KILLED, SANDBOX_DELETED), SAVE)
          .put(new TestCase(true, KILLED, DELETED), DELETE_TASK)
          .put(new TestCase(true, KILLING, ASSIGNED), ILLEGAL_KILL)
          .put(new TestCase(false, KILLING, ASSIGNED), ILLEGAL_KILL)
          .put(new TestCase(true, KILLING, STARTING), ILLEGAL_KILL)
          .put(new TestCase(false, KILLING, STARTING), ILLEGAL_KILL)
          .put(new TestCase(true, KILLING, RUNNING), ILLEGAL_KILL)
          .put(new TestCase(false, KILLING, RUNNING), ILLEGAL_KILL)
          .put(new TestCase(true, KILLING, FINISHED), SAVE)
          .put(new TestCase(true, KILLING, FAILED), SAVE)
          .put(new TestCase(true, KILLING, KILLED), SAVE)
          .put(new TestCase(true, KILLING, LOST), SAVE)
          .put(new TestCase(true, KILLING, SANDBOX_DELETED), SAVE)
          .put(new TestCase(true, KILLING, DELETED), DELETE_TASK)
          .put(new TestCase(true, LOST, ASSIGNED), ILLEGAL_KILL)
          .put(new TestCase(false, LOST, ASSIGNED), ILLEGAL_KILL)
          .put(new TestCase(true, LOST, STARTING), ILLEGAL_KILL)
          .put(new TestCase(false, LOST, STARTING), ILLEGAL_KILL)
          .put(new TestCase(true, LOST, RUNNING), ILLEGAL_KILL)
          .put(new TestCase(false, LOST, RUNNING), ILLEGAL_KILL)
          .put(new TestCase(true, LOST, SANDBOX_DELETED), SAVE)
          .put(new TestCase(true, LOST, DELETED), DELETE_TASK)
          .put(new TestCase(true, SANDBOX_DELETED, ASSIGNED), ILLEGAL_KILL)
          .put(new TestCase(false, SANDBOX_DELETED, ASSIGNED), ILLEGAL_KILL)
          .put(new TestCase(true, SANDBOX_DELETED, STARTING), ILLEGAL_KILL)
          .put(new TestCase(false, SANDBOX_DELETED, STARTING), ILLEGAL_KILL)
          .put(new TestCase(true, SANDBOX_DELETED, RUNNING), ILLEGAL_KILL)
          .put(new TestCase(false, SANDBOX_DELETED, RUNNING), ILLEGAL_KILL)
          .put(new TestCase(true, SANDBOX_DELETED, DELETED), DELETE_TASK)
          .put(new TestCase(false, DELETED, ASSIGNED), ILLEGAL_KILL)
          .put(new TestCase(false, DELETED, STARTING), ILLEGAL_KILL)
          .put(new TestCase(false, DELETED, RUNNING), ILLEGAL_KILL)
          .build();

  @Test
  public void testAllTransitions() {
    for (TaskState from : TaskState.values()) {
      for (TaskState to : TaskState.values()) {
        for (Boolean taskPresent : ImmutableList.of(Boolean.TRUE, Boolean.FALSE)) {
          TestCase testCase = new TestCase(taskPresent, from, to);

          TransitionResult expectation = EXPECTATIONS.get(testCase);
          if (expectation == null) {
            expectation = new TransitionResult(false, ImmutableSet.<SideEffect>of());
          }

          TaskStateMachine machine;
          if (taskPresent) {
            // Cannot create a state machine for an DELETED task that is in the store.
            boolean expectException = from == DELETED;
            try {
              machine = new TaskStateMachine(
                  IScheduledTask.build(makeTask(false).setStatus(from.getStatus().get())));
              if (expectException) {
                fail();
              }
            } catch (IllegalStateException e) {
              if (!expectException) {
                throw e;
              } else {
                continue;
              }
            }
          } else {
            machine = new TaskStateMachine("name");
          }

          assertEquals(
              "Unexpected behavior for " + testCase,
              expectation,
              machine.updateState(to.getStatus()));
        }
      }
    }
  }
}
