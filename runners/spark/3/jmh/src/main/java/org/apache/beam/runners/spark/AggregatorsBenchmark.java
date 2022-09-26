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
package org.apache.beam.runners.spark;

import static org.apache.beam.runners.spark.structuredstreaming.translation.helpers.EncoderHelpers.encoderFor;
import static org.apache.beam.runners.spark.structuredstreaming.translation.helpers.EncoderHelpers.encoderOf;
import static org.apache.beam.runners.spark.structuredstreaming.translation.helpers.EncoderHelpers.windowedValueEncoder;

import java.util.Random;
import org.apache.beam.runners.spark.structuredstreaming.translation.batch.Aggregators;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.transforms.windowing.Sessions;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.expressions.Aggregator;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

public class AggregatorsBenchmark {

  @State(Scope.Benchmark)
  public static class Partition {
    private static final Combine.CombineFn<Long, long[], Long> COMBINE_FN = Sum.ofLongs();

    private static final Duration GAP_DURATION = Duration.millis(5);

    private static final Encoder<IntervalWindow> WINDOW_ENCODER;
    private static final Encoder<WindowedValue<Long>> OUTPUT_ENC;
    private static final Encoder<long[]> ACC_ENCODER;

    static {
      try {
        WINDOW_ENCODER = encoderOf(IntervalWindow.class);
        OUTPUT_ENC = windowedValueEncoder(encoderOf(Long.class), WINDOW_ENCODER);
        ACC_ENCODER = encoderFor(COMBINE_FN.getAccumulatorCoder(null, VarLongCoder.of()));
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    @Param({"0.1", "0.2", "0.4"})
    double newSessionProbability;

    @Param({"true", "false"})
    boolean ordered;

    @Param({"true", "false"})
    boolean optimize;

    Random random;

    Aggregator<WindowedValue<Long>, ?, ?> aggregator;

    Object aggregationBuffer;

    Instant maxTimestamp = Instant.EPOCH;

    @Setup(Level.Iteration)
    public void setup() {
      random = new Random(0); // deterministic, repeatable input sequence
      Sessions sessions = Sessions.withGapDuration(GAP_DURATION);
      WindowingStrategy<?, ?> windowing = WindowingStrategy.of(sessions);
      if (optimize) {
        aggregator =
            new Aggregators.SessionsAggregator<>(
                COMBINE_FN,
                WindowedValue::getValue,
                windowing,
                WINDOW_ENCODER,
                ACC_ENCODER,
                OUTPUT_ENC);
      } else {
        aggregator =
            new Aggregators.MergingWindowedAggregator<Long, long[], Long, Long>(
                COMBINE_FN,
                WindowedValue::getValue,
                windowing,
                ((Encoder) WINDOW_ENCODER),
                ACC_ENCODER,
                OUTPUT_ENC);
      }
      ;
      aggregationBuffer = aggregator.zero();
    }

    WindowedValue<Long> nextWindowedValue() {
      if (random.nextDouble() < newSessionProbability) {
        // force new session
        maxTimestamp = maxTimestamp.plus(GAP_DURATION).plus(Duration.millis(1));
        return windowedValue(maxTimestamp);
      } else if (ordered) {
        // add the latest session
        maxTimestamp = maxTimestamp.plus(Duration.millis(1));
        return windowedValue(maxTimestamp);
      } else {
        // add to any previous session
        long startMillis = (long) (maxTimestamp.getMillis() * random.nextDouble());
        return windowedValue(Instant.ofEpochMilli(startMillis));
      }
    }

    private static WindowedValue<Long> windowedValue(Instant start) {
      Instant end = start.plus(GAP_DURATION);
      BoundedWindow window = new IntervalWindow(start, end);
      return WindowedValue.of(start.getMillis(), start, window, PaneInfo.NO_FIRING);
    }

    void reduce(WindowedValue<Long> wv) {
      aggregationBuffer =
          ((Aggregator<WindowedValue<Long>, Object, ?>) aggregator).reduce(aggregationBuffer, wv);
    }
  }

  @Benchmark
  public void reducePartition(Partition partition) {
    partition.reduce(partition.nextWindowedValue());
  }
}
