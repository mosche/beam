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

import java.util.Collection;
import org.apache.beam.runners.reactor.translation.TransformTranslator;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import reactor.core.publisher.Flux;

class FlattenTranslatorBatch<T>
    extends TransformTranslator<PCollectionList<T>, PCollection<T>, Flatten.PCollections<T>> {

  @Override
  @SuppressWarnings("nullness")
  public void translate(Flatten.PCollections<T> transform, Context cxt) {
    Collection<PCollection<T>> values = (Collection) cxt.getInputs().values();
    Flux<Flux<WindowedValue<T>>> flattened = null;

    for (PCollection<T> pCol : values) {
      if (flattened == null) {
        flattened = (Flux<Flux<WindowedValue<T>>>) cxt.require(pCol);
      } else {
        flattened = flattened.concatWith(cxt.require(pCol));
      }
    }

    if (flattened == null) {
      flattened = Flux.empty();
    }
    cxt.provide(cxt.getOutput(), flattened);
  }
}
