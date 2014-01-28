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

import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import javax.inject.Inject;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.eventbus.Subscribe;
import com.google.inject.BindingAnnotation;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.common.util.Clock;

import org.apache.aurora.gen.apiConstants;
import org.apache.aurora.scheduler.async.MappedFutures.ScheduledMappedFutures;
import org.apache.aurora.scheduler.base.Query;
import org.apache.aurora.scheduler.base.Tasks;
import org.apache.aurora.scheduler.state.StateManager;
import org.apache.aurora.scheduler.storage.Storage;
import org.apache.aurora.scheduler.storage.entities.IJobKey;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

import static com.google.common.base.Preconditions.checkNotNull;

import static org.apache.aurora.scheduler.events.PubsubEvent.EventSubscriber;
import static org.apache.aurora.scheduler.events.PubsubEvent.TaskStateChange;
import static org.apache.aurora.scheduler.events.PubsubEvent.TasksDeleted;

/**
 * Prunes tasks in a job based on per-job history and an inactive time threshold by observing tasks
 * transitioning into one of the inactive states.
 */
public class HistoryPruner implements EventSubscriber {
  private static final Logger LOG = Logger.getLogger(HistoryPruner.class.getName());

  private final StateManager stateManager;
  private final Clock clock;
  private final long pruneThresholdMillis;
  private final int perJobHistoryGoal;
  private final Storage storage;
  private final ExecutorService executor;
  private final ScheduledMappedFutures<String> pendingDeletes;

  @BindingAnnotation
  @Target({ FIELD, PARAMETER, METHOD }) @Retention(RUNTIME)
  public @interface PruneThreshold { }

  @Inject
  HistoryPruner(
      final ScheduledExecutorService executor,
      final StateManager stateManager,
      final Clock clock,
      @PruneThreshold Amount<Long, Time> inactivePruneThreshold,
      @PruneThreshold int perJobHistoryGoal,
      Storage storage) {

    this.stateManager = checkNotNull(stateManager);
    this.clock = checkNotNull(clock);
    this.pruneThresholdMillis = inactivePruneThreshold.as(Time.MILLISECONDS);
    this.perJobHistoryGoal = perJobHistoryGoal;
    this.storage = checkNotNull(storage);
    this.executor = checkNotNull(executor);
    this.pendingDeletes = new ScheduledMappedFutures<>(executor);
  }

  @VisibleForTesting
  long calculateTimeout(long taskEventTimestampMillis) {
    return pruneThresholdMillis - Math.max(0, clock.nowMillis() - taskEventTimestampMillis);
  }

  /**
   * When triggered, records an inactive task state change.
   *
   * @param change Event when a task changes state.
   */
  @Subscribe
  public void recordStateChange(TaskStateChange change) {
    if (Tasks.isTerminated(change.getNewState())) {
      long timeoutBasis = change.isTransition()
          ? clock.nowMillis()
          : Iterables.getLast(change.getTask().getTaskEvents()).getTimestamp();
      registerInactiveTask(
          Tasks.SCHEDULED_TO_JOB_KEY.apply(change.getTask()),
          change.getTaskId(),
          calculateTimeout(timeoutBasis));
    }
  }

  private void deleteTasks(Set<String> taskIds) {
    LOG.info("Pruning inactive tasks " + taskIds);
    stateManager.deleteTasks(taskIds);
  }

  /**
   * When triggered, removes the tasks scheduled for pruning and cancels any existing future.
   *
   * @param event A new TasksDeleted event.
   */
  @Subscribe
  public void tasksDeleted(final TasksDeleted event) {
    pendingDeletes.cancel(Tasks.ids(event.getTasks()));
  }

  private void registerInactiveTask(
      final IJobKey jobKey,
      final String taskId,
      long timeRemaining) {

    LOG.fine("Prune task " + taskId + " in " + timeRemaining + " ms.");
    // Insert the latest inactive task at the tail.

    // TODO(wfarner): Consider reworking this class to not cache tasksByJob, and map scheduled
    // futures by job key.  Upon the task being triggered, it will prune the job down to the
    // appropriate history size.
    pendingDeletes.schedule(
        taskId,
        new Runnable() {
          @Override public void run() {
            LOG.info("Pruning expired inactive task " + taskId);
            deleteTasks(ImmutableSet.of(taskId));
          }
        },
        timeRemaining,
        TimeUnit.MILLISECONDS);

    executor.submit(new Runnable() {
      @Override public void run() {
        Collection<IScheduledTask> inactiveTasks = Storage.Util.weaklyConsistentFetchTasks(
            storage,
            Query.jobScoped(jobKey).byStatus(apiConstants.TERMINAL_STATES));
        int tasksToPrune = inactiveTasks.size() - perJobHistoryGoal;
        if (tasksToPrune > 0) {
          if (inactiveTasks.size() > perJobHistoryGoal) {
            Set<String> toPrune = FluentIterable
                .from(Tasks.LATEST_ACTIVITY.reverse().sortedCopy(inactiveTasks))
                .limit(tasksToPrune)
                .transform(Tasks.SCHEDULED_TO_ID)
                .toSet();
            pendingDeletes.cancel(toPrune);
            deleteTasks(toPrune);
          }
        }
      }
    });
  }
}
