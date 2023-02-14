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
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.junit.Rule;
import org.junit.Test;

import java.io.Serializable;

public class FlattenTest implements Serializable {
  @Rule public TestPipeline pipeline = TestPipeline.fromOptions(TestOptions.create());

  @Test
  public void testFlatten() {
    PCollection<Integer> in1 = pipeline.apply("in1", Create.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));
    PCollection<Integer> in2 =
        pipeline.apply("in2", Create.of(11, 12, 13, 14, 15, 16, 17, 18, 19, 20));
    PCollectionList<Integer> pcs = PCollectionList.of(in1).and(in2);
    PCollection<Integer> input = pcs.apply(Flatten.pCollections());
    PAssert.that(input)
        .containsInAnyOrder(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20);
    pipeline.run();
  }
}
