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

import org.apache.beam.runners.reactor.translation.TransformTranslator;
import org.apache.beam.runners.reactor.translation.Translation;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.util.IdentityWindowFn;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions;

class AssignWindowTranslatorBatch<T>
    extends TransformTranslator<PCollection<T>, PCollection<T>, Window.Assign<T>> {

  @Override
  protected boolean canTranslate(Window.Assign<T> transform) {
    Preconditions.checkState(
        transform.getWindowFn() instanceof GlobalWindows
            || transform.getWindowFn() instanceof IdentityWindowFn,
        "Only default WindowingStrategy supported");
    return true;
  }

  @Override
  public void translate(Window.Assign<T> transform, Context cxt) {
    cxt.translate(cxt.getOutput(), Translation.identity());
  }
}
