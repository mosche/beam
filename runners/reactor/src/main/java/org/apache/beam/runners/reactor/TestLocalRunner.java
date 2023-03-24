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

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineRunner;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.testing.TestPipelineOptions;

@Experimental
public final class TestLocalRunner extends PipelineRunner<LocalPipelineResult> {
  private final LocalRunner delegate;

  public static TestLocalRunner fromOptions(PipelineOptions options) {
    return new TestLocalRunner(options);
  }

  private TestLocalRunner(PipelineOptions options) {
    this.delegate = LocalRunner.fromOptions(options);
  }

  @Override
  public LocalPipelineResult run(Pipeline pipeline) {
    LocalPipelineResult result = delegate.run(pipeline);
    TestPipelineOptions testOptions = pipeline.getOptions().as(TestPipelineOptions.class);
    if (testOptions.isBlockOnRun()) {
      result.waitUntilFinish();
    }
    return result;
  }
}
