/**
 * Copyright 2014 Twitter, Inc.
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

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

/**
 * TODO(wfarner): Consider putting this in a different package.
 */
public class MappedFutures<K> {

  private final Set<K> activeKeys =
      Collections.newSetFromMap(Maps.<K, Boolean>newConcurrentMap());
  private final ExecutorService executor;

  public MappedFutures(ExecutorService executor) {
    this.executor = Preconditions.checkNotNull(executor);
  }

  public boolean submit(K key, Runnable work) {
    boolean submitting = add(key);
    if (submitting) {
      executor.submit(runOnceIfValid(key, work));
    }
    return submitting;
  }

  protected boolean add(K key) {
    return activeKeys.add(key);
  }

  protected Runnable runOnceIfValid(final K key, final Runnable work) {
    return new Runnable() {
      @Override public void run() {
        if (activeKeys.remove(key)) {
          work.run();
        }
      }
    };
  }

  public boolean cancel(K key) {
    return activeKeys.remove(key);
  }

  public void cancel(Set<K> keys) {
    activeKeys.removeAll(keys);
  }

  public static class ScheduledMappedFutures<K> extends MappedFutures<K> {
    private final ScheduledExecutorService scheduledExecutor;

    public ScheduledMappedFutures(ScheduledExecutorService executorService) {
      super(executorService);
      this.scheduledExecutor = executorService;
    }

    public boolean schedule(K key, Runnable work, long delay, TimeUnit timeUnit) {
      boolean submitting = add(key);
      if (submitting) {
        scheduledExecutor.schedule(runOnceIfValid(key, work), delay, timeUnit);
      }
      return submitting;
    }
  }
}
