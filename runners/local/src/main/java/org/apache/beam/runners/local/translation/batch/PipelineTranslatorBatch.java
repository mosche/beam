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

import org.apache.beam.runners.core.construction.SplittableParDo;
import org.apache.beam.runners.local.translation.PipelineTranslator;
import org.apache.beam.runners.local.translation.TransformTranslator;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.Impulse;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.HashMap;
import java.util.Map;

@Internal
public class PipelineTranslatorBatch extends PipelineTranslator {

  @SuppressWarnings("rawtypes")
  private static final Map<Class<? extends PTransform>, TransformTranslator> TRANSLATORS =
      new HashMap<>();

  static {
    TRANSLATORS.put(Impulse.class, new ImpulseTranslatorBatch());
    TRANSLATORS.put(Combine.PerKey.class, new CombinePerKeyTranslatorBatch<>());
    TRANSLATORS.put(Combine.Globally.class, new CombineGloballyTranslatorBatch<>());
    TRANSLATORS.put(Combine.GroupedValues.class, new CombineGroupedValuesTranslatorBatch<>());
    TRANSLATORS.put(GroupByKey.class, new GroupByKeyTranslatorBatch<>());

    TRANSLATORS.put(Reshuffle.class, new ReshuffleTranslatorBatch<>());
    TRANSLATORS.put(Reshuffle.ViaRandomKey.class, new ReshuffleTranslatorBatch.ViaRandomKey<>());

    TRANSLATORS.put(Flatten.PCollections.class, new FlattenTranslatorBatch<>());

    TRANSLATORS.put(Window.Assign.class, new AssignWindowTranslatorBatch<>());

    TRANSLATORS.put(ParDo.MultiOutput.class, new ParDoTranslatorBatch<>());
    TRANSLATORS.put(SplittableParDo.PrimitiveBoundedRead.class, new ReadSourceTranslatorBatch<>());
  }

  @Override
  protected <InT extends PInput, OutT extends POutput, T extends PTransform<InT, OutT>> @Nullable
      TransformTranslator<InT, OutT, T> getTransformTranslator(T transform) {
    return TRANSLATORS.get(transform.getClass());
  }
}
