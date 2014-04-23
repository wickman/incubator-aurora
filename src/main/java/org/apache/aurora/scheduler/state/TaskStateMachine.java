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

import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;

import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.twitter.common.base.Closure;
import com.twitter.common.base.Closures;
import com.twitter.common.base.Command;
import com.twitter.common.base.MorePreconditions;
import com.twitter.common.stats.Stats;
import com.twitter.common.util.StateMachine;
import com.twitter.common.util.StateMachine.Rule;
import com.twitter.common.util.StateMachine.Transition;

import org.apache.aurora.gen.ScheduleStatus;
import org.apache.aurora.scheduler.base.Tasks;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;

import static com.google.common.base.Preconditions.checkNotNull;

import static org.apache.aurora.scheduler.state.SideEffect.Action;
import static org.apache.aurora.scheduler.state.SideEffect.Action.DELETE;
import static org.apache.aurora.scheduler.state.SideEffect.Action.INCREMENT_FAILURES;
import static org.apache.aurora.scheduler.state.SideEffect.Action.KILL;
import static org.apache.aurora.scheduler.state.SideEffect.Action.RESCHEDULE;
import static org.apache.aurora.scheduler.state.SideEffect.Action.SAVE_STATE;
import static org.apache.aurora.scheduler.state.SideEffect.Action.STATE_CHANGE;
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

/**
 * State machine for a task.
 * <p>
 * This enforces the lifecycle of a task, and triggers the actions that should be taken in response
 * to different state transitions.  These responses are externally communicated by populating a
 * provided work queue.
 * <p>
 * TODO(wfarner): Augment this class to force the one-time-use nature.  This is probably best done
 * by hiding the constructor and exposing only a static function to transition a task and get the
 * resulting actions.
 */
class TaskStateMachine {
  private static final Logger LOG = Logger.getLogger(TaskStateMachine.class.getName());

  private static final AtomicLong ILLEGAL_TRANSITIONS =
      Stats.exportLong("scheduler_illegal_task_state_transitions");

  private final StateMachine<TaskState> stateMachine;
  private Optional<TaskState> previousState = Optional.absent();

  private final Set<SideEffect> sideEffects = Sets.newHashSet();

  private static final Function<ScheduleStatus, TaskState> STATUS_TO_TASK_STATE =
      new Function<ScheduleStatus, TaskState>() {
        @Override
        public TaskState apply(ScheduleStatus input) {
          return TaskState.valueOf(input.name());
        }
      };

  private static final Function<IScheduledTask, TaskState> SCHEDULED_TO_TASK_STATE =
      Functions.compose(STATUS_TO_TASK_STATE, Tasks.GET_STATUS);

  /**
   * ScheduleStatus enum extension to account for cases where no direct state mapping exists.
   * TODO:(maxim): Consider making this private.
   */
  @VisibleForTesting
  enum TaskState {
    INIT(Optional.of(ScheduleStatus.INIT)),
    THROTTLED(Optional.of(ScheduleStatus.THROTTLED)),
    PENDING(Optional.of(ScheduleStatus.PENDING)),
    ASSIGNED(Optional.of(ScheduleStatus.ASSIGNED)),
    STARTING(Optional.of(ScheduleStatus.STARTING)),
    RUNNING(Optional.of(ScheduleStatus.RUNNING)),
    FINISHED(Optional.of(ScheduleStatus.FINISHED)),
    PREEMPTING(Optional.of(ScheduleStatus.PREEMPTING)),
    RESTARTING(Optional.of(ScheduleStatus.RESTARTING)),
    DRAINING(Optional.of(ScheduleStatus.DRAINING)),
    FAILED(Optional.of(ScheduleStatus.FAILED)),
    KILLED(Optional.of(ScheduleStatus.KILLED)),
    KILLING(Optional.of(ScheduleStatus.KILLING)),
    LOST(Optional.of(ScheduleStatus.LOST)),
    SANDBOX_DELETED(Optional.of(ScheduleStatus.SANDBOX_DELETED)),
    /**
     * The task does not have an associated state as it has been deleted from the store.
     */
    DELETED(Optional.<ScheduleStatus>absent());

    private final Optional<ScheduleStatus> status;

    TaskState(Optional<ScheduleStatus> status) {
      this.status = status;
    }

    Optional<ScheduleStatus> getStatus() {
      return status;
    }
  }

  /**
   * Creates a new task state machine representing a non-existent task.  This allows for consistent
   * state-reconciliation actions when the external system disagrees with the scheduler.
   *
   * @param name Name of the state machine, for logging.
   */
  public TaskStateMachine(String name) {
    this(name, Optional.<IScheduledTask>absent());
  }

  /**
   * Creates a new task state machine representing an existent task.  The state machine will be
   * named with the tasks ID.
   *.
   * @param task Read-only task that this state machine manages.
   */
  public TaskStateMachine(IScheduledTask task) {
    this(Tasks.id(task), Optional.of(task));
  }

  private TaskStateMachine(final String name, final Optional<IScheduledTask> task) {
    MorePreconditions.checkNotBlank(name);
    checkNotNull(task);

    final TaskState initialState = task.transform(SCHEDULED_TO_TASK_STATE).or(DELETED);
    if (task.isPresent()) {
      Preconditions.checkState(
          initialState != DELETED,
          "A task that exists may not be in DELETED state.");
    } else {
      Preconditions.checkState(
          initialState == DELETED,
          "A task that does not exist must start in DELETED state.");
    }

    Closure<Transition<TaskState>> manageTerminatedTasks = Closures.combine(
        ImmutableList.<Closure<Transition<TaskState>>>builder()
            // Kill a task that we believe to be terminated when an attempt is made to revive.
            .add(
                Closures.filter(Transition.to(ASSIGNED, STARTING, RUNNING),
                    addFollowupClosure(KILL)))
            // Remove a terminated task that is requested to be deleted.
            .add(Closures.filter(Transition.to(DELETED), addFollowupClosure(DELETE)))
            .build());

    final Closure<Transition<TaskState>> manageRestartingTask =
        new Closure<Transition<TaskState>>() {
          @Override
          public void execute(Transition<TaskState> transition) {
            switch (transition.getTo()) {
              case ASSIGNED:
                addFollowup(KILL);
                break;

              case STARTING:
                addFollowup(KILL);
                break;

              case RUNNING:
                addFollowup(KILL);
                break;

              case LOST:
                addFollowup(KILL);
                addFollowup(RESCHEDULE);
                break;

              case FINISHED:
                addFollowup(RESCHEDULE);
                break;

              case FAILED:
                addFollowup(RESCHEDULE);
                break;

              case KILLED:
                addFollowup(RESCHEDULE);
                break;

              case SANDBOX_DELETED:
                addFollowupTransition(LOST);
                break;

              default:
                // No-op.
            }
          }
        };

    // To be called on a task transitioning into the FINISHED state.
    final Command rescheduleIfService = new Command() {
      @Override
      public void execute() {
        if (task.get().getAssignedTask().getTask().isIsService()) {
          addFollowup(RESCHEDULE);
        }
      }
    };

    // To be called on a task transitioning into the FAILED state.
    final Command incrementFailuresMaybeReschedule = new Command() {
      @Override
      public void execute() {
        addFollowup(INCREMENT_FAILURES);

        // Max failures is ignored for service task.
        boolean isService = task.get().getAssignedTask().getTask().isIsService();

        // Max failures is ignored when set to -1.
        int maxFailures = task.get().getAssignedTask().getTask().getMaxTaskFailures();
        boolean belowMaxFailures =
            (maxFailures == -1) || (task.get().getFailureCount() < (maxFailures - 1));
        if (isService || belowMaxFailures) {
          addFollowup(RESCHEDULE);
        } else {
          LOG.info("Task " + name + " reached failure limit, not rescheduling");
        }
      }
    };

    final Closure<Transition<TaskState>> deleteIfKilling =
        Closures.filter(Transition.to(KILLING), addFollowupClosure(DELETE));

    stateMachine = StateMachine.<TaskState>builder(name)
        .logTransitions()
        .initialState(initialState)
        .addState(
            Rule.from(INIT)
                .to(PENDING, THROTTLED))
        .addState(
            Rule.from(PENDING)
                .to(ASSIGNED, KILLING)
                .withCallback(deleteIfKilling))
        .addState(
            Rule.from(THROTTLED)
            .to(PENDING, KILLING)
            .withCallback(deleteIfKilling))
        .addState(
            Rule.from(ASSIGNED)
                .to(STARTING, RUNNING, FINISHED, FAILED, RESTARTING, DRAINING,
                    KILLED, KILLING, LOST, PREEMPTING)
                .withCallback(
                    new Closure<Transition<TaskState>>() {
                      @Override
                      public void execute(Transition<TaskState> transition) {
                        switch (transition.getTo()) {
                          case FINISHED:
                            rescheduleIfService.execute();
                            break;

                          case PREEMPTING:
                            addFollowup(KILL);
                            break;

                          case FAILED:
                            incrementFailuresMaybeReschedule.execute();
                            break;

                          case RESTARTING:
                            addFollowup(KILL);
                            break;

                          case DRAINING:
                            addFollowup(KILL);
                            break;

                          case KILLED:
                            addFollowup(RESCHEDULE);
                            break;

                          case LOST:
                            addFollowup(RESCHEDULE);
                            addFollowup(KILL);
                            break;

                          case KILLING:
                            addFollowup(KILL);
                            break;

                          default:
                            // No-op.
                        }
                      }
                    }
                ))
        .addState(
            Rule.from(STARTING)
                .to(RUNNING, FINISHED, FAILED, RESTARTING, DRAINING, KILLING,
                    KILLED, LOST, PREEMPTING)
                .withCallback(
                    new Closure<Transition<TaskState>>() {
                      @Override
                      public void execute(Transition<TaskState> transition) {
                        switch (transition.getTo()) {
                          case FINISHED:
                            rescheduleIfService.execute();
                            break;

                          case RESTARTING:
                            addFollowup(KILL);
                            break;

                          case DRAINING:
                            addFollowup(KILL);
                            break;

                          case PREEMPTING:
                            addFollowup(KILL);
                            break;

                          case FAILED:
                            incrementFailuresMaybeReschedule.execute();
                            break;

                          case KILLED:
                            addFollowup(RESCHEDULE);
                            break;

                          case KILLING:
                            addFollowup(KILL);
                            break;

                          case LOST:
                            addFollowup(RESCHEDULE);
                            break;

                          case SANDBOX_DELETED:
                            // The slave previously acknowledged that it had the task, and now
                            // stopped reporting it.
                            addFollowupTransition(LOST);
                            break;

                          default:
                            // No-op.
                        }
                      }
                    }
                ))
        .addState(
            Rule.from(RUNNING)
                .to(FINISHED, RESTARTING, DRAINING, FAILED, KILLING, KILLED, LOST, PREEMPTING)
                .withCallback(
                    new Closure<Transition<TaskState>>() {
                      @Override
                      public void execute(Transition<TaskState> transition) {
                        switch (transition.getTo()) {
                          case FINISHED:
                            rescheduleIfService.execute();
                            break;

                          case PREEMPTING:
                            addFollowup(KILL);
                            break;

                          case RESTARTING:
                            addFollowup(KILL);
                            break;

                          case DRAINING:
                            addFollowup(KILL);
                            break;

                          case FAILED:
                            incrementFailuresMaybeReschedule.execute();
                            break;

                          case KILLED:
                            addFollowup(RESCHEDULE);
                            break;

                          case KILLING:
                            addFollowup(KILL);
                            break;

                          case LOST:
                            addFollowup(RESCHEDULE);
                            break;

                          case SANDBOX_DELETED:
                            addFollowupTransition(LOST);
                            break;

                           default:
                             // No-op.
                        }
                      }
                    }
                ))
        .addState(
            Rule.from(FINISHED)
                .to(SANDBOX_DELETED, DELETED)
                .withCallback(manageTerminatedTasks))
        .addState(
            Rule.from(PREEMPTING)
                .to(FINISHED, FAILED, KILLING, KILLED, LOST)
                .withCallback(manageRestartingTask))
        .addState(
            Rule.from(RESTARTING)
                .to(FINISHED, FAILED, KILLING, KILLED, LOST)
                .withCallback(manageRestartingTask))
        .addState(
            Rule.from(DRAINING)
                .to(FINISHED, FAILED, KILLING, KILLED, LOST)
                .withCallback(manageRestartingTask))
        .addState(
            Rule.from(FAILED)
                .to(SANDBOX_DELETED, DELETED)
                .withCallback(manageTerminatedTasks))
        .addState(
            Rule.from(KILLED)
                .to(SANDBOX_DELETED, DELETED)
                .withCallback(manageTerminatedTasks))
        // TODO(maxim): Re-evaluate if *DELETED states are valid transitions here.
        .addState(
            Rule.from(KILLING)
                .to(FINISHED, FAILED, KILLED, LOST, SANDBOX_DELETED, DELETED)
                .withCallback(manageTerminatedTasks))
        .addState(
            Rule.from(LOST)
                .to(SANDBOX_DELETED, DELETED)
                .withCallback(manageTerminatedTasks))
        .addState(
            Rule.from(SANDBOX_DELETED)
                .to(DELETED)
                .withCallback(manageTerminatedTasks))
        .addState(
            Rule.from(DELETED)
                .noTransitions()
                .withCallback(manageTerminatedTasks))
        // Since we want this action to be performed last in the transition sequence, the callback
        // must be the last chained transition callback.
        .onAnyTransition(
            new Closure<Transition<TaskState>>() {
              @Override
              public void execute(final Transition<TaskState> transition) {
                if (transition.isValidStateChange()) {
                  TaskState from = transition.getFrom();
                  TaskState to = transition.getTo();

                  // TODO(wfarner): Clean up this hack.  This is here to suppress unnecessary work
                  // (save followed by delete), but it shows a wart with this catch-all behavior.
                  // Strongly consider pushing the SAVE_STATE behavior to each transition handler.
                  boolean pendingDeleteHack =
                      !(((from == PENDING) || (from == THROTTLED)) && (to == KILLING));

                  // Don't bother saving state of a task that is being removed.
                  if ((to != DELETED) && pendingDeleteHack) {
                    addFollowup(SAVE_STATE);
                  }
                  previousState = Optional.of(from);
                } else {
                  LOG.severe("Illegal state transition attempted: " + transition);
                  ILLEGAL_TRANSITIONS.incrementAndGet();
                }
              }
            }
        )
        // TODO(wfarner): Consider alternatives to allow exceptions to surface.  This would allow
        // the state machine to surface illegal state transitions and propagate better information
        // to the caller.  As it stands, the caller must implement logic that really belongs in
        // the state machine.  For example, preventing RESTARTING->UPDATING transitions
        // (or for that matter, almost any user-initiated state transition) is awkward.
        .throwOnBadTransition(false)
        .build();
  }

  private void addFollowup(Action action) {
    addFollowup(new SideEffect(action, Optional.<ScheduleStatus>absent()));
  }

  private void addFollowupTransition(TaskState state) {
    addFollowup(new SideEffect(STATE_CHANGE, state.getStatus()));
  }

  private void addFollowup(SideEffect sideEffect) {
    LOG.info("Adding work command " + sideEffect + " for " + this);
    sideEffects.add(sideEffect);
  }

  private Closure<Transition<TaskState>> addFollowupClosure(final Action action) {
    return new Closure<Transition<TaskState>>() {
      @Override
      public void execute(Transition<TaskState> item) {
        addFollowup(action);
      }
    };
  }

  /**
   * Attempt to transition the state machine to the provided state.
   * At the time this method returns, any work commands required to satisfy the state transition
   * will be appended to the work queue.
   *
   * TODO(maxim): The current StateManager/TaskStateMachine interaction makes it hard to expose
   * a dedicated task deletion method without leaking out the state machine implementation details.
   * Consider refactoring here to allow for an unambiguous task deletion without resorting to
   * Optional.absent().
   *
   * @param status Status to apply to the task or absent if a task deletion is required.
   * @return {@code true} if the state change was allowed, {@code false} otherwise.
   */
  public synchronized TransitionResult updateState(final Optional<ScheduleStatus> status) {
    checkNotNull(status);
    Preconditions.checkState(sideEffects.isEmpty());

    /**
     * Don't bother applying noop state changes.  If we end up modifying task state without a
     * state transition (e.g. storing resource consumption of a running task), we need to find
     * a different way to suppress noop transitions.
     */
    TaskState taskState = status.transform(STATUS_TO_TASK_STATE).or(TaskState.DELETED);
    if (stateMachine.getState() == taskState) {
      return new TransitionResult(false, ImmutableSet.<SideEffect>of());
    }

    boolean success = stateMachine.transition(taskState);
    ImmutableSet<SideEffect> transitionEffects = ImmutableSet.copyOf(sideEffects);
    sideEffects.clear();
    return new TransitionResult(success, transitionEffects);
  }

  /**
   * Gets the previous state of this state machine.
   *
   * @return The state machine's previous state, or {@code null} if the state machine has not
   *     transitioned since being created.
   */
  @Nullable
  ScheduleStatus getPreviousState() {
    return previousState.transform(new Function<TaskState, ScheduleStatus>() {
      @Override
      public ScheduleStatus apply(TaskState item) {
        return item.getStatus().orNull();
      }
    }).orNull();
  }

  @Override
  public String toString() {
    return stateMachine.getName();
  }
}
