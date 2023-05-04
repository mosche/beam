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

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.DefaultValueFactory;
import org.apache.beam.sdk.options.PipelineOptions;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

public interface LocalPipelineOptions extends PipelineOptions {
  enum SDFMode {
    SYNC,
    ASYNC
  }

  @Default.Boolean(false)
  boolean isMetricsEnabled();

  void setMetricsEnabled(boolean enable);

  @Default.Enum("ASYNC")
  SDFMode getSDFMode();

  void setSDFMode(SDFMode mode);

  @Default.Integer(1)
  int getParallelism();

  void setParallelism(int parallelism);

  @Default.InstanceFactory(SchedulerFactory.class)
  Scheduler getScheduler();

  void setScheduler(Scheduler scheduler);

  class SchedulerFactory implements DefaultValueFactory<Scheduler> {
    @Override
    public Scheduler create(PipelineOptions options) {
      return Schedulers.parallel();
    }
  }
}
