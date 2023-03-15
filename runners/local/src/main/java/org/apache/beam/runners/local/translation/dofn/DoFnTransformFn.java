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
package org.apache.beam.runners.local.translation.dofn;

import java.util.ArrayDeque;
import java.util.Spliterator;
import java.util.function.Consumer;
import org.apache.beam.runners.core.DoFnRunners;
import org.apache.beam.runners.local.LocalPipelineOptions;
import org.apache.beam.runners.local.translation.Dataset.TransformFn;
import org.apache.beam.runners.local.translation.dofn.DoFnRunnerFactory.DoFnRunnerWithTeardown;
import org.apache.beam.sdk.metrics.MetricsContainer;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.TupleTag;
import org.checkerframework.checker.nullness.qual.Nullable;

public class DoFnTransformFn<T1, T2> implements TransformFn<T1, T2> {
  private final LocalPipelineOptions opts;
  private final MetricsContainer metrics;
  private final DoFnRunnerFactory<T1, T2> factory;

  public DoFnTransformFn(
      LocalPipelineOptions opts, MetricsContainer metrics, DoFnRunnerFactory<T1, T2> factory) {
    this.opts = opts;
    this.metrics = metrics;
    this.factory = factory;
  }

  @Override
  public Spliterator<WindowedValue<T2>> apply(Spliterator<WindowedValue<T1>> in) {
    return new DoFnSpliterator<>(in, opts, metrics, factory);
  }

  @Override
  public <T3> TransformFn<T1, T3> fuse(TransformFn<T2, T3> fn) {
    if (fn instanceof DoFnTransformFn) {
      return new DoFnTransformFn<>(
          opts, metrics, factory.fuse(((DoFnTransformFn<T2, T3>) fn).factory));
    }
    return TransformFn.super.fuse(fn);
  }

  /** Collect is required for splittable ParDos so processing can be parallelized. */
  @Override
  public boolean requiresCollect() {
    return factory.requiresCollect();
  }

  private static class DoFnSpliterator<InT, OutT>
      implements Spliterator<WindowedValue<OutT>>, DoFnRunners.OutputManager {

    final ArrayDeque<WindowedValue<OutT>> buffer = new ArrayDeque<>();
    final Spliterator<WindowedValue<InT>> input;
    final int characteristics;
    final DoFnRunnerWithTeardown<InT, OutT> runner;
    boolean isFinished = false;

    @SuppressWarnings("argument")
    DoFnSpliterator(
        Spliterator<WindowedValue<InT>> input,
        LocalPipelineOptions opts,
        MetricsContainer metrics,
        DoFnRunnerFactory<InT, OutT> factory) {
      this.input = input;
      this.characteristics = input.characteristics() & ~Spliterator.SIZED;
      this.runner = factory.create(opts, metrics, this);
    }

    @Override
    public @Nullable Spliterator<WindowedValue<OutT>> trySplit() {
      return null; // if needed, delegate split to input
    }

    @Override
    public boolean tryAdvance(Consumer<? super WindowedValue<OutT>> action) {
      try {
        while (true) {
          WindowedValue<OutT> first = buffer.pollFirst();
          if (first != null) {
            action.accept(first);
            return true;
          }
          if (!isFinished) {
            if (input.tryAdvance(runner::processElement)) {
              // continue with loop to consume from buffer
            } else {
              isFinished = true;
              runner.finishBundle();
            }
          } else {
            runner.teardown();
            return false;
          }
        }
      } catch (RuntimeException e) {
        runner.teardown();
        throw e;
      }
    }

    @Override
    public long estimateSize() {
      return input.estimateSize();
    }

    @Override
    public int characteristics() {
      return characteristics;
    }

    @Override
    public <T> void output(TupleTag<T> tag, WindowedValue<T> output) {
      buffer.addLast((WindowedValue<OutT>) output);
    }
  }
}
