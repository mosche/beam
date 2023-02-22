/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.runners.local.translation;

import static java.util.Collections.EMPTY_LIST;

import java.util.Collection;
import java.util.List;
import java.util.Spliterator;
import java.util.concurrent.CountedCompleter;
import javax.annotation.Nullable;
import org.apache.beam.sdk.util.WindowedValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class TransformTask<InT, AccT, OutT> {
  private static final Logger LOG = LoggerFactory.getLogger(TransformTask.class);

  protected final String name;
  private List<Spliterator<WindowedValue<InT>>> splits = EMPTY_LIST;

  protected TransformTask(String name) {
    this.name = name;
  }

  public OutT evaluate(Dataset<InT> dataset) {
    this.splits = dataset.spliterators();
    LOG.info("Evaluating {} on {}: {}", name, dataset.name, dataset);
    return new ParallelTask(null, null, 0, splits.size()).invoke();
  }

  protected abstract AccT add(@Nullable AccT acc, WindowedValue<InT> wv, int idx);

  protected abstract AccT merge(AccT left, AccT right);

  protected abstract OutT getOutput(@Nullable AccT acc);

  private class ParallelTask extends CountedCompleter<OutT> {
    private final int lo, hi;
    private ParallelTask forks, next; // record subtask forks in list
    private @Nullable AccT acc;
    private int counter = 0;

    ParallelTask(@Nullable ParallelTask parent, @Nullable ParallelTask next, int lo, int hi) {
      super(parent);
      this.lo = lo;
      this.hi = hi;
      this.next = next;
    }

    @Override
    public OutT getRawResult() {
      OutT output = getOutput(acc);
      if (output instanceof Collection) {
        Collection<?> col = (Collection<?>) output;
        LOG.info("Output of {}: records: {}", name, col.size());
      }
      return output;
    }

    @Override
    public void compute() {
      int l = lo, h = hi;
      while (h - l >= 2) {
        int mid = (l + h) >>> 1;
        addToPendingCount(1);
        (forks = new ParallelTask(this, forks, mid, h)).fork();
        h = mid;
      }
      if (h > l) {
        Spliterator<WindowedValue<InT>> split = splits.get(l);
        LOG.info(
            "Starting {} for split {} / {} [estimated: {}, exact: {}]",
            name,
            l + 1,
            splits.size(),
            split.estimateSize(),
            split.getExactSizeIfKnown());
        split.forEachRemaining(v -> acc = add(acc, v, counter++));
        LOG.info(
            "Completed {} for split {} / {} [inputs: {}]", name, l + 1, splits.size(), counter);
      }
      // process completions by reducing along and advancing subtask links
      for (CountedCompleter<?> c = firstComplete(); c != null; c = c.nextComplete()) {
        for (ParallelTask t = (ParallelTask) c, s = t.forks; s != null; s = t.forks = s.next) {
          if (s.acc != null) {
            t.acc = (t.acc != null) ? merge(t.acc, s.acc) : s.acc;
          }
        }
      }
    }
  }
}
