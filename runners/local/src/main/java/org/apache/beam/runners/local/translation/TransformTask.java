package org.apache.beam.runners.local.translation;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Spliterator;
import java.util.concurrent.CountedCompleter;
import java.util.function.Consumer;

public abstract class TransformTask<
        InT, AccT, OutT, TaskT extends TransformTask<InT, AccT, OutT, TaskT>>
    extends CountedCompleter<OutT> {
  public interface Factory<T1, T2>{
    TransformTask<T1, ?, T2, ?> create(List<Spliterator<T1>> splits);
  }

  private final List<Spliterator<InT>> splits;
  private final int lo, hi;
  private TaskT forks, next; // record subtask forks in list
  private @Nullable AccT acc;

  protected TransformTask(
      @Nullable TaskT parent, @Nullable TaskT next, List<Spliterator<InT>> splits, int lo, int hi) {
    super(parent);
    this.splits = splits;
    this.lo = lo;
    this.hi = hi;
    this.next = next;
  }

  protected abstract TaskT subTask(
      TaskT parent, TaskT next, List<Spliterator<InT>> splits, int lo, int hi);

  protected abstract AccT add(@Nullable AccT acc, InT in);

  protected abstract AccT merge(AccT left, AccT right);

  protected abstract OutT getOutput(@Nullable AccT acc);

  @Override
  public OutT getRawResult() {
    return getOutput(acc);
  }

  @Override
  public void compute() {
    int l = lo, h = hi;
    while (h - l >= 2) {
      int mid = (l + h) >>> 1;
      addToPendingCount(1);
      (forks = subTask((TaskT) this, forks, splits, mid, h)).fork();
      h = mid;
    }
    if (h > l) {
      Consumer<InT> addToAcc = v -> acc = add(acc, v);
      splits.get(l).forEachRemaining(addToAcc);
    }
    // process completions by reducing along and advancing subtask links
    for (CountedCompleter<?> c = firstComplete(); c != null; c = c.nextComplete()) {
      for (TransformTask<InT, AccT, OutT, TaskT> t = (TaskT) c, s = t.forks;
          s != null;
          s = t.forks = s.next) {
        if (s.acc != null) {
          if (t.acc != null) {
            t.acc = merge(t.acc, s.acc);
          } else {
            t.acc = s.acc;
          }
        }
      }
    }
  }
}
