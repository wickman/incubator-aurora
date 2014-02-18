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
package org.apache.aurora.scheduler.http;

import java.util.Collection;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.Iterables;
import com.google.common.collect.Range;

import org.apache.aurora.scheduler.base.Numbers;
import org.apache.aurora.scheduler.storage.entities.IPackage;

/**
 * Utility class to hold common object to string transformation helper functions.
 */
final class TransformationUtils {
  public static final Function<IPackage, String> PACKAGE_TOSTRING =
      new Function<IPackage, String>() {
        @Override
        public String apply(IPackage pkg) {
          return pkg.getRole() + "/" + pkg.getName() + " v" + pkg.getVersion();
        }
      };

  public static final Function<Range<Integer>, String> RANGE_TOSTRING =
      new Function<Range<Integer>, String>() {
        @Override
        public String apply(Range<Integer> range) {
          int lower = range.lowerEndpoint();
          int upper = range.upperEndpoint();
          return (lower == upper) ? String.valueOf(lower) : (lower + " - " + upper);
        }
      };

  public static final Function<Collection<Integer>, String> INSTANCES_TOSTRING =
      new Function<Collection<Integer>, String>() {
        @Override
        public String apply(Collection<Integer> instances) {
          return Joiner.on(", ")
              .join(Iterables.transform(Numbers.toRanges(instances), RANGE_TOSTRING));
        }
      };

  private TransformationUtils() {
    // Utility class
  }
}
