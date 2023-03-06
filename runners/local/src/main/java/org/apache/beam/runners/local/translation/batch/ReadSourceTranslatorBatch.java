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

import static org.apache.beam.sdk.util.WindowedValue.timestampedValueInGlobalWindow;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Spliterator;
import java.util.function.Consumer;
import javax.annotation.Nullable;
import org.apache.beam.runners.core.construction.SplittableParDo;
import org.apache.beam.runners.local.translation.Dataset;
import org.apache.beam.runners.local.translation.TransformTranslator;
import org.apache.beam.runners.local.translation.utils.Spliterable;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.BoundedSource.BoundedReader;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("unused")
class ReadSourceTranslatorBatch<T>
    extends TransformTranslator<PBegin, PCollection<T>, SplittableParDo.PrimitiveBoundedRead<T>> {

  private static final Logger LOG = LoggerFactory.getLogger(ReadSourceTranslatorBatch.class);

  @Override
  public void translate(SplittableParDo.PrimitiveBoundedRead<T> transform, Context cxt)
      throws Exception {
    PipelineOptions opts = cxt.getOptions();
    BoundedSource<T> source = transform.getSource();
    long desiredSize = source.getEstimatedSizeBytes(opts) / (2 ^ cxt.getOptions().getSplits());

    List<? extends BoundedSource<T>> splits = source.split(desiredSize, opts);
    LOG.info("Reading from {} using {} splits.", transform.getName(), splits.size());

    List<SpliterableSource<T>> spliterators = new ArrayList<>(splits.size());
    for (BoundedSource<T> splitSource : splits) {
      spliterators.add(new SpliterableSource<>(splitSource, opts));
    }

    cxt.provideDataset(cxt.getOutput(), Dataset.ofSplits(cxt.fullName(), spliterators));
  }

  private static class SpliterableSource<T> implements Spliterable<WindowedValue<T>> {
    final BoundedSource<T> source;
    final PipelineOptions options;

    SpliterableSource(BoundedSource<T> source, PipelineOptions options) {
      this.source = source;
      this.options = options;
    }

    private BoundedReader<T> createReader() {
      try {
        return source.createReader(options);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public Spliterator<WindowedValue<T>> spliterator() {
      return new BoundedReaderSpliterator<>(createReader());
    }

    private static class BoundedReaderSpliterator<T> implements Spliterator<WindowedValue<T>> {
      boolean isStarted = false;
      final BoundedReader<T> reader;

      BoundedReaderSpliterator(BoundedReader<T> reader) {
        this.reader = reader;
      }

      private boolean start() throws IOException {
        isStarted = true;
        return reader.start();
      }

      @Override
      public boolean tryAdvance(Consumer<? super WindowedValue<T>> action) {
        try {
          if (isStarted ? reader.advance() : start()) {
            action.accept(
                timestampedValueInGlobalWindow(reader.getCurrent(), reader.getCurrentTimestamp()));
            return true;
          }
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
        return false;
      }

      @Override
      public @Nullable Spliterator<WindowedValue<T>> trySplit() {
        return null;
      }

      @Override
      public long estimateSize() {
        return Long.MAX_VALUE;
      }

      @Override
      public int characteristics() {
        return Spliterator.IMMUTABLE | Spliterator.NONNULL;
      }
    }
  }
}
