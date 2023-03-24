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
package org.apache.beam.runners.reactor;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.beam.runners.core.metrics.MetricsContainerStepMap;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.metrics.MetricResults;
import org.apache.beam.sdk.util.UserCodeException;
import org.joda.time.Duration;

public class LocalPipelineResult implements PipelineResult {
  private final MetricsContainerStepMap metrics;

  private final Future<Void> completion;

  public LocalPipelineResult(MetricsContainerStepMap metrics, Future<Void> completion) {
    this.metrics = metrics;
    this.completion = completion;
  }

  @Override
  public PipelineResult.State getState() {
    if (completion.isCancelled()) {
      return State.CANCELLED;
    } else if (completion.isDone()) {
      try {
        completion.get();
        return State.DONE;
      } catch (Exception e) {
        return State.FAILED;
      }
    }
    return State.RUNNING;
  }

  @Override
  public PipelineResult.State waitUntilFinish() {
    try {
      completion.get();
      return State.DONE;
    } catch (ExecutionException e) {
      throw beamExceptionFrom(e);
    } catch (InterruptedException e) {
      throw runtimeExceptionFrom(e);
    }
  }

  @Override
  public State waitUntilFinish(final Duration duration) {
    try {
      completion.get(duration.getMillis(), TimeUnit.MILLISECONDS);
      return State.DONE;
    } catch (TimeoutException e) {
      return State.RUNNING;
    } catch (ExecutionException e) {
      throw beamExceptionFrom(e);
    } catch (InterruptedException e) {
      throw runtimeExceptionFrom(e);
    }
  }

  @SuppressWarnings("nullness")
  private static RuntimeException beamExceptionFrom(ExecutionException e) {
    if (e.getCause() != null && e.getCause() instanceof UserCodeException) {
      UserCodeException userException = (UserCodeException) e.getCause();
      return new Pipeline.PipelineExecutionException(userException.getCause());
    } else if (e.getCause() != null) {
      return new Pipeline.PipelineExecutionException(e.getCause());
    }
    return runtimeExceptionFrom(e);
  }

  private static RuntimeException runtimeExceptionFrom(final Throwable e) {
    return (e instanceof RuntimeException) ? (RuntimeException) e : new RuntimeException(e);
  }

  @Override
  public MetricResults metrics() {
    MetricResults metricResults = MetricsContainerStepMap.asAttemptedOnlyMetricResults(metrics);
    return metricResults;
  }

  @Override
  public PipelineResult.State cancel() {
    // FIXME to be implemented
    return getState();
  }
}
