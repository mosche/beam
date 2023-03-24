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
package org.apache.beam.runners.reactor.translation.batch;

import org.apache.beam.runners.reactor.translation.PipelineTranslator.Translation;
import org.apache.beam.runners.reactor.translation.TransformTranslator;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Scheduler;

class CombineGloballyTranslatorBatch<InT, AccT, OutT>
    extends TransformTranslator<PCollection<InT>, PCollection<OutT>, Combine.Globally<InT, OutT>> {

  @Override
  protected void translate(Combine.Globally<InT, OutT> transform, Context cxt) {
    CombineFn<InT, AccT, OutT> fn = (CombineFn<InT, AccT, OutT>) transform.getFn();
    Scheduler scheduler = cxt.getScheduler();
    int parallelism = cxt.getOptions().getParallelism();
    cxt.translate(cxt.getOutput(), new TranslateCombineGlobally<>(fn, scheduler, parallelism));
  }

  private static class TranslateCombineGlobally<InT, AccT, OutT> implements Translation<InT, OutT> {
    final CombineFn<InT, AccT, OutT> fn;
    final Scheduler scheduler;
    final int parallelism;

    TranslateCombineGlobally(CombineFn<InT, AccT, OutT> fn, Scheduler scheduler, int parallelism) {
      this.fn = fn;
      this.scheduler = scheduler;
      this.parallelism = parallelism;
    }

    private AccT add(AccT acc, WindowedValue<InT> wv) {
      return wv.getValue() == null ? acc : fn.addInput(acc, wv.getValue());
    }

    @Override
    public Flux<Flux<WindowedValue<OutT>>> apply(Flux<? extends Flux<WindowedValue<InT>>> flux) {
      Flux<WindowedValue<OutT>> global =
          flux.subscribeOn(scheduler)
              .flatMap(f -> f.reduce(fn.createAccumulator(), this::add), parallelism)
              .reduce(this::merge)
              .map(fn::extractOutput)
              .map(WindowedValue::valueInGlobalWindow)
              .flux();
      return Flux.just(global.subscribeOn(scheduler));
    }

    private AccT merge(AccT acc1, AccT acc2) {
      return fn.mergeAccumulators(ImmutableList.of(acc1, acc2));
    }
  }
}
