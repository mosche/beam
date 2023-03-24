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

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static java.util.stream.IntStream.range;
import static org.apache.beam.sdk.transforms.windowing.BoundedWindow.TIMESTAMP_MIN_VALUE;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.IntStream;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.junit.Rule;
import org.junit.Test;

public class ReshuffleTest {

  @Rule public transient TestPipeline pipeline = TestPipeline.fromOptions(TestOptions.create());

  private IntStream input() {
    return range(0, 100);
  }

  @Test
  public void testReshuffleByKey() {
    int mod = 5; // independent of partitions / parallelism
    List<KV<Integer, Integer>> values = input().mapToObj(i -> KV.of(i % mod, i)).collect(toList());

    PCollection<Set<Integer>> result =
        pipeline
            .apply(Create.of(values))
            .apply(Reshuffle.of())
            .apply(ParDo.of(VALUES_BY_PARTITION));

    PAssert.that(result)
        .containsInAnyOrder(range(0, mod).mapToObj(i -> moduloSet(mod, i)).collect(toList()));
    pipeline.run();
  }

  private Set<Integer> moduloSet(int mod, int remainder) {
    return input().filter(i -> i % mod == remainder).boxed().collect(toSet());
  }

  private static final DoFn<KV<Integer, Integer>, Set<Integer>> VALUES_BY_PARTITION =
      new DoFn<KV<Integer, Integer>, Set<Integer>>() {
        Map<Integer, Set<Integer>> map = ImmutableMap.of();

        @StartBundle
        public void startBundle() {
          map = new HashMap<>();
        }

        @FinishBundle
        public void finishBundle(FinishBundleContext c) {
          map.values().forEach(set -> c.output(set, TIMESTAMP_MIN_VALUE, GlobalWindow.INSTANCE));
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
          map.compute(
              c.element().getKey(),
              (k, set) -> {
                set = set == null ? new HashSet<>() : set;
                set.add(c.element().getValue());
                return set;
              });
        }
      };
}
