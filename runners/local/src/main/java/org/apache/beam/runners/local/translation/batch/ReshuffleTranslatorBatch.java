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

import java.io.IOException;
import org.apache.beam.runners.local.translation.Dataset;
import org.apache.beam.runners.local.translation.TransformTranslator;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

class ReshuffleTranslatorBatch<K, V>
    extends TransformTranslator<PCollection<KV<K, V>>, PCollection<KV<K, V>>, Reshuffle<K, V>> {

  @Override
  protected void translate(Reshuffle<K, V> transform, Context cxt) throws IOException {
    Dataset<KV<K, V>> dataset = cxt.requireDataset(cxt.getInput());
    cxt.provideDataset(cxt.getOutput(), dataset); // noop
  }

  static class ViaRandomKey<V>
      extends TransformTranslator<PCollection<V>, PCollection<V>, Reshuffle.ViaRandomKey<V>> {

    @Override
    protected void translate(Reshuffle.ViaRandomKey<V> transform, Context cxt) throws IOException {
      Dataset<V> dataset = cxt.requireDataset(cxt.getInput());
      cxt.provideDataset(cxt.getOutput(), dataset); // noop
    }
  }
}
