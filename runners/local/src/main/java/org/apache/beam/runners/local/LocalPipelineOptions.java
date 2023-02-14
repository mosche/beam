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
package org.apache.beam.runners.local;

import java.math.RoundingMode;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.DefaultValueFactory;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.math.IntMath;

public interface LocalPipelineOptions extends PipelineOptions {

  @Description("Number of fork-join splits resulting in 2^splits partitions")
  @Default.InstanceFactory(DefaultSplits.class)
  int getSplits();

  void setSplits(int splits);

  @Default.Boolean(false)
  boolean isMetricsEnabled();

  void setMetricsEnabled(boolean enable);

  class DefaultSplits implements DefaultValueFactory<Integer> {
    @Override
    public Integer create(PipelineOptions options) {
      //return IntMath.log2(Runtime.getRuntime().availableProcessors(), RoundingMode.CEILING);
      return 2;
    }
  }
}
