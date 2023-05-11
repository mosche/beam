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

import java.util.List;
import java.util.concurrent.Future;
import org.apache.beam.runners.core.construction.PTransformMatchers;
import org.apache.beam.runners.core.construction.SplittableParDo;
import org.apache.beam.runners.core.construction.SplittableParDoNaiveBounded;
import org.apache.beam.runners.core.metrics.MetricsContainerStepMap;
import org.apache.beam.runners.reactor.translation.batch.PipelineTranslatorBatch;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineRunner;
import org.apache.beam.sdk.metrics.MetricsEnvironment;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsValidator;
import org.apache.beam.sdk.runners.PTransformOverride;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;

public final class ReactorRunner extends PipelineRunner<ReactorPipelineResult> {
  private final ReactorOptions options;

  public static ReactorRunner fromOptions(PipelineOptions options) {
    return new ReactorRunner(PipelineOptionsValidator.validate(ReactorOptions.class, options));
  }

  private ReactorRunner(ReactorOptions options) {
    this.options = options;
  }

  @Override
  public ReactorPipelineResult run(final Pipeline pipeline) {
    MetricsEnvironment.setMetricsSupported(options.isMetricsEnabled());
    pipeline.replaceAll(getDefaultOverrides());
    MetricsContainerStepMap metrics = new MetricsContainerStepMap();
    Future<Void> completion = new PipelineTranslatorBatch().evaluate(pipeline, options, metrics);
    return new ReactorPipelineResult(metrics, completion);
  }

  private static List<PTransformOverride> getDefaultOverrides() {
    return ImmutableList.of(
        PTransformOverride.of(
            PTransformMatchers.splittableParDo(), new SplittableParDo.OverrideFactory<>()),
        PTransformOverride.of(
            PTransformMatchers.splittableProcessKeyedBounded(),
            new SplittableParDoNaiveBounded.OverrideFactory<>()));
  }
}
