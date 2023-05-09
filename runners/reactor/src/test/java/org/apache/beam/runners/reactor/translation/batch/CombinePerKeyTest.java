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

import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Distinct;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.junit.Rule;
import org.junit.Test;

public class CombinePerKeyTest {
  @Rule public TestPipeline pipeline = TestPipeline.fromOptions(TestOptions.create());

  PCollection<KV<Integer, Integer>> summedPerKey() {
    List<KV<Integer, Integer>> elems = new ArrayList<>();
    elems.add(KV.of(1, 1));
    elems.add(KV.of(1, 3));
    elems.add(KV.of(1, 5));
    elems.add(KV.of(2, 2));
    elems.add(KV.of(2, 4));
    elems.add(KV.of(2, 6));

    return pipeline.apply(Create.of(elems)).apply(Sum.integersPerKey());
  }

  @Test
  public void testCombinePerKey() {
    PCollection<KV<Integer, Integer>> input = summedPerKey();
    PAssert.that(input).containsInAnyOrder(KV.of(1, 9), KV.of(2, 12));
    pipeline.run();
  }

  @Test
  public void testCombinePerKeyWithMultipleSubscribers() {
    PCollection<KV<Integer, Integer>> input = summedPerKey();
    PAssert.that(input).containsInAnyOrder(KV.of(1, 9), KV.of(2, 12));
    PAssert.that(input).containsInAnyOrder(KV.of(1, 9), KV.of(2, 12));
    pipeline.run();
  }

  @Test
  public void testDistinctViaCombinePerKey() {
    List<Integer> elems = Lists.newArrayList(1, 2, 3, 3, 4, 4, 4, 4, 5, 5);

    // Distinct is implemented in terms of CombinePerKey
    PCollection<Integer> result = pipeline.apply(Create.of(elems)).apply(Distinct.create());

    PAssert.that(result).containsInAnyOrder(1, 2, 3, 4, 5);
    pipeline.run();
  }
}
