/**
 * Copyright (c) 2010-2016 Yahoo! Inc., 2017
 * YCSB contributors All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

package site.ycsb;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Parsed representation of a throughput schedule that can override the global
 * {@code -target} at specific time windows during a run.
 *
 * <p>Specification format (property {@code throttle.schedule}):</p>
 * <pre>
 *   start:stop:tps[,start:stop:tps...]
 * </pre>
 * where {@code start} and {@code stop} are times relative to run start and may
 * use units {@code ms|s|m|h} (default seconds), and {@code tps} is the total
 * target operations per second for that window.
 *
 * <p>Outside all windows, callers should fall back to the base {@code -target}
 * if set; otherwise the run is unthrottled.</p>
 */
public final class ThroughputSchedule {

  /**
   * A single time window and its target TPS.
   */
  public static final class Slot {
    private final long startMs; // relative to t0
    private final long stopMs;  // exclusive
    private final double tps;   // total target ops/sec

    /**
     * Constructs a slot.
     *
     * @param startMs start time (inclusive) in milliseconds relative to t0
     * @param stopMs  stop time (exclusive) in milliseconds relative to t0
     * @param tps     total target ops/sec for the window
     */
    public Slot(long startMs, long stopMs, double tps) {
      this.startMs = startMs;
      this.stopMs = stopMs;
      this.tps = tps;
    }

    /**
     * @return true if the given elapsed time falls inside this slot.
     */
    public boolean contains(long elapsedMs) {
      return elapsedMs >= startMs && elapsedMs < stopMs;
    }

    /** @return start time (inclusive) in ms relative to t0. */
    public long getStartMs() {
      return startMs;
    }

    /** @return stop time (exclusive) in ms relative to t0. */
    public long getStopMs() {
      return stopMs;
    }

    /** @return target ops/sec for this window. */
    public double getTps() {
      return tps;
    }

    @Override
    public String toString() {
      return "Slot{" + startMs + "→" + stopMs + " @ " + tps + " tps}";
    }
  }

  private final List<Slot> slots; // sorted, non-overlapping

  private ThroughputSchedule(List<Slot> slots) {
    this.slots = slots;
  }

  /**
   * Parse a schedule specification string.
   *
   * @param spec schedule string or {@code null}/empty
   * @return a {@link ThroughputSchedule} or {@code null} if spec is null/empty
   */
  public static ThroughputSchedule parse(String spec) {
    if (spec == null || spec.trim().isEmpty()) {
      return null;
    }

    String[] parts = spec.split("\\s*,\\s*");
    List<Slot> tmp = new ArrayList<Slot>(parts.length);
    for (String p : parts) {
      String[] f = p.split("\\s*:\\s*");
      if (f.length != 3) {
        throw new IllegalArgumentException("Bad slot: " + p);
      }

      long start = parseDurationMs(f[0]);
      long stop = parseDurationMs(f[1]);
      double tps = Double.parseDouble(f[2]);

      if (start < 0 || stop <= start || tps <= 0) {
        throw new IllegalArgumentException("Invalid values in slot: " + p);
      }
      tmp.add(new Slot(start, stop, tps));
    }

    Collections.sort(tmp, new Comparator<Slot>() {
      @Override
      public int compare(Slot a, Slot b) {
        if (a.getStartMs() < b.getStartMs()) {
          return -1;
        } else if (a.getStartMs() > b.getStartMs()) {
          return 1;
        }
        return 0;
      }
    });

    for (int i = 1; i < tmp.size(); i++) {
      if (tmp.get(i - 1).getStopMs() > tmp.get(i).getStartMs()) {
        throw new IllegalArgumentException(
            "Overlapping slots: " + tmp.get(i - 1) + " and " + tmp.get(i));
      }
    }

    return new ThroughputSchedule(Collections.unmodifiableList(tmp));
  }

  /**
   * Return the effective TPS for the given elapsed time, or the provided
   * default value if outside all windows.
   *
   * @param elapsedMs elapsed time since run start in ms
   * @param defaultTps default TPS (may be {@code null} meaning unthrottled)
   * @return active slot TPS, {@code defaultTps}, or {@code +Inf} for unthrottled
   */
  public double tpsForElapsedMs(long elapsedMs, Double defaultTps) {
    for (Slot s : slots) {
      if (s.contains(elapsedMs)) {
        return s.getTps();
      }
    }
    if (defaultTps == null) {
      return Double.POSITIVE_INFINITY;
    }
    return defaultTps.doubleValue();
  }

  /**
   * @return immutable list of slots in start-time order.
   */
  public List<Slot> slots() {
    return slots;
  }

  private static long parseDurationMs(String s) {
    String in = s.trim().toLowerCase(Locale.ROOT);
    Pattern pat = Pattern.compile("^(\\d+)(ms|s|m|h)?$");
    Matcher m = pat.matcher(in);
    if (!m.matches()) {
      throw new IllegalArgumentException("Bad duration: " + s);
    }
    long n = Long.parseLong(m.group(1));
    String u = m.group(2);
    if (u == null || "s".equals(u)) {
      return n * 1000L;
    }
    if ("ms".equals(u)) {
      return n;
    }
    if ("m".equals(u)) {
      return n * 60_000L;
    }
    if ("h".equals(u)) {
      return n * 3_600_000L;
    }
    throw new IllegalArgumentException("Bad unit: " + s);
  }
}
