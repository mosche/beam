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

import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.junit.Rule;
import org.junit.Test;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.beam.sdk.testing.SerializableMatchers.containsInAnyOrder;
import static org.hamcrest.MatcherAssert.assertThat;

public class GroupByKeyTest implements Serializable {
  @Rule public TestPipeline pipeline = TestPipeline.fromOptions(TestOptions.create());

  @Test
  public void testGroupByKey() {
    List<KV<Integer, Integer>> elems =
        shuffleRandomly(
            KV.of(1, 1), KV.of(1, 3), KV.of(1, 5), KV.of(2, 2), KV.of(2, 4), KV.of(2, 6));

    PCollection<KV<Integer, Iterable<Integer>>> input =
        pipeline.apply(Create.of(elems)).apply(GroupByKey.create());

    PAssert.thatMap(input)
        .satisfies(
            results -> {
              assertThat(results.get(1), containsInAnyOrder(1, 3, 5));
              assertThat(results.get(2), containsInAnyOrder(2, 4, 6));
              return null;
            });
    pipeline.run();
  }

  private <T> List<T> shuffleRandomly(T... elems) {
    ArrayList<T> list = Lists.newArrayList(elems);
    Collections.shuffle(list);
    return list;
  }
}
