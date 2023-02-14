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

import org.apache.beam.runners.local.translation.Dataset;
import org.apache.beam.runners.local.translation.TransformTask;
import org.apache.beam.runners.local.translation.TransformTranslator;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.List;
import java.util.Spliterator;

class CombineGloballyTranslatorBatch<InT, AccT, OutT>
    extends TransformTranslator<PCollection<InT>, PCollection<OutT>, Combine.Globally<InT, OutT>> {

  @Override
  protected void translate(Combine.Globally<InT, OutT> transform, Context cxt) {
    CombineFn<InT, AccT, OutT> fn = (CombineFn<InT, AccT, OutT>) transform.getFn();
    Dataset<InT> dataset = cxt.getDataset(cxt.getInput());
    cxt.putDataset(cxt.getOutput(), dataset.transform(splits -> new CombineTask<>(splits, fn)));
  }

  private static class CombineTask<InT, AccT, OutT>
      extends TransformTask<WindowedValue<InT>, AccT, Collection<WindowedValue<OutT>>, CombineTask<InT, AccT, OutT>> {
    final CombineFn<InT, AccT, OutT> fn;

    CombineTask(List<Spliterator<WindowedValue<InT>>> splits, CombineFn<InT, AccT, OutT> fn) {
      this(null, null, splits, 0, splits.size(), fn);
    }

    private CombineTask(
        @Nullable CombineTask<InT, AccT, OutT> parent,
        @Nullable CombineTask<InT, AccT, OutT> next,
        List<Spliterator<WindowedValue<InT>>> splits,
        int lo,
        int hi,
        CombineFn<InT, AccT, OutT> fn) {
      super(parent, next, splits, lo, hi);
      this.fn = fn;
    }

    @Override
    protected CombineTask<InT, AccT, OutT> subTask(
        CombineTask<InT, AccT, OutT> parent,
        CombineTask<InT, AccT, OutT> next,
        List<Spliterator<WindowedValue<InT>>> splits,
        int lo,
        int hi) {
      return new CombineTask<>(parent, next, splits, lo, hi, fn);
    }

    @Override
    protected AccT add(@Nullable AccT acc, WindowedValue<InT> wv) {
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
