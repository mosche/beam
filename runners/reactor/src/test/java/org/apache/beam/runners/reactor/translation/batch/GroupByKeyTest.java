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

import static org.apache.beam.sdk.testing.SerializableMatchers.containsInAnyOrder;
import static org.hamcrest.MatcherAssert.assertThat;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.transforms.CoGroup;
import org.apache.beam.sdk.schemas.transforms.CoGroup.By;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.junit.Rule;
import org.junit.Test;

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

  @Test
  public void testCoGroupJoin() {
    Schema schemaA = Schema.builder().addStringField("ida").addStringField("val").build();
    Schema schemaB = Schema.builder().addStringField("idb").addStringField("val").build();
    Schema resSchema = Schema.builder().addRowField("a", schemaA).addRowField("b", schemaB).build();

    Row id1a1 = Row.withSchema(schemaA).addValues("id1", "a1").build();
    Row id2a2 = Row.withSchema(schemaA).addValues("id2", "a2").build();
    Row id1b1 = Row.withSchema(schemaB).addValues("id1", "b1").build();
    Row id2b2 = Row.withSchema(schemaB).addValues("id2", "b2").build();
    Row id2b3 = Row.withSchema(schemaB).addValues("id2", "b3").build();

    PCollection<Row> inA = pipeline.apply("a", Create.of(id1a1, id2a2)).setRowSchema(schemaA);
    PCollection<Row> inB =
        pipeline.apply("b", Create.of(id1b1, id2b2, id2b3)).setRowSchema(schemaB);

    PCollection<Row> joined =
        PCollectionTuple.of("a", inA, "b", inB)
            .apply(
                CoGroup.join("a", By.fieldNames("ida"))
                    .join("b", By.fieldNames("idb"))
                    .crossProductJoin());
    // .apply(CoGroup.join(CoGroup.By.fieldNames("id")).crossProductJoin());

    PAssert.that(joined)
        .containsInAnyOrder(
            Row.withSchema(resSchema).addValues(id1a1, id1b1).build(),
            Row.withSchema(resSchema).addValues(id2a2, id2b2).build(),
            Row.withSchema(resSchema).addValues(id2a2, id2b3).build());

    pipeline.run();
  }

  private <T> List<T> shuffleRandomly(T... elems) {
    ArrayList<T> list = Lists.newArrayList(elems);
    Collections.shuffle(list);
    return list;
  }
}
