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

import static org.apache.beam.repackaged.core.org.apache.commons.lang3.ArrayUtils.EMPTY_BYTE_ARRAY;
import static org.apache.beam.sdk.util.WindowedValue.valueInGlobalWindow;

import org.apache.beam.runners.reactor.translation.TransformTranslator;
import org.apache.beam.sdk.transforms.Impulse;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import reactor.core.publisher.Flux;

class ImpulseTranslatorBatch extends TransformTranslator<PBegin, PCollection<byte[]>, Impulse> {

  @Override
  public void translate(Impulse transform, Context cxt) {
    Flux<WindowedValue<byte[]>> impulse = Flux.just(valueInGlobalWindow(EMPTY_BYTE_ARRAY));
    cxt.provide(cxt.getOutput(), Flux.just(impulse.subscribeOn(cxt.getScheduler())));
  }
}
