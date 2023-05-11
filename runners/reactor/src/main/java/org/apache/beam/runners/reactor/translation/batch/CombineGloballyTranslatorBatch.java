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

import org.apache.beam.runners.reactor.ReactorOptions;
import org.apache.beam.runners.reactor.translation.TransformTranslator;
import org.apache.beam.runners.reactor.translation.Translation;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollection;
import reactor.core.publisher.Flux;

class CombineGloballyTranslatorBatch<InT, AccT, OutT>
    extends TransformTranslator<PCollection<InT>, PCollection<OutT>, Combine.Globally<InT, OutT>> {

  @Override
  protected void translate(
      Context<PCollection<InT>, PCollection<OutT>, Combine.Globally<InT, OutT>> cxt) {
    CombineFn<InT, AccT, OutT> fn = (CombineFn<InT, AccT, OutT>) cxt.getTransform().getFn();
    cxt.translate(cxt.getOutput(), new TranslateCombineGlobally<>(fn));
  }

  private static class TranslateCombineGlobally<InT, AccT, OutT>
      extends Translation.BasicTranslation<InT, OutT> {
    final CombineFn<InT, AccT, OutT> fn;

    TranslateCombineGlobally(CombineFn<InT, AccT, OutT> fn) {
      this.fn = fn;
    }

    private AccT add(AccT acc, WindowedValue<InT> wv) {
      return wv.getValue() == null ? acc : fn.addInput(acc, wv.getValue());
    }

    @Override
    public Flux<WindowedValue<OutT>> simple(Flux<WindowedValue<InT>> flux, ReactorOptions opts) {
      return flux.reduce(fn.createAccumulator(), this::add)
          .map(fn::extractOutput)
          .map(WindowedValue::valueInGlobalWindow)
          .flux();
    }
  }
}
