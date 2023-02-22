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
package org.apache.beam.runners.local.translation.batch;

import java.util.Collection;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.beam.runners.local.translation.Dataset;
import org.apache.beam.runners.local.translation.TransformTask;
import org.apache.beam.runners.local.translation.TransformTranslator;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;

class CombineGloballyTranslatorBatch<InT, AccT, OutT>
    extends TransformTranslator<PCollection<InT>, PCollection<OutT>, Combine.Globally<InT, OutT>> {

  @Override
  protected void translate(Combine.Globally<InT, OutT> transform, Context cxt) {
    CombineFn<InT, AccT, OutT> fn = (CombineFn<InT, AccT, OutT>) transform.getFn();
    Dataset<InT> inputDs = cxt.requireDataset(cxt.getInput());
    Dataset<OutT> resultDs = inputDs.evaluate(new CombineTask<>(cxt.fullName(), fn));
    cxt.provideDataset(cxt.getOutput(), resultDs);
  }

  private static class CombineTask<InT, AccT, OutT>
      extends TransformTask<InT, AccT, Collection<WindowedValue<OutT>>> {
    final CombineFn<InT, AccT, OutT> fn;

    private CombineTask(String name, CombineFn<InT, AccT, OutT> fn) {
      super(name);
      this.fn = fn;
    }

    @Override
    protected AccT add(@Nullable AccT acc, WindowedValue<InT> wv, int idx) {
      return fn.addInput(acc != null ? acc : fn.createAccumulator(), wv.getValue());
    }

    @Override
    protected AccT merge(AccT left, AccT right) {
      return fn.mergeAccumulators(ImmutableList.of(left, right));
    }

    @Override
    protected List<WindowedValue<OutT>> getOutput(@Nullable AccT acc) {
      OutT out = acc != null ? fn.extractOutput(acc) : fn.defaultValue();
      return ImmutableList.of(WindowedValue.valueInGlobalWindow(out));
    }
  }
}
