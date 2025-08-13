/**
 * Copyright (c) 2010-2016 Yahoo! Inc., 2017
 * YCSB contributors All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS"
 * BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

package site.ycsb;

import site.ycsb.measurements.Measurements;

import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

/**
 * A thread to periodically show the status of the experiment to reassure you
 * that progress is being made.
 *
 * This version also displays the active throttle.schedule slot (if any) and its TPS.
 */
public class StatusThread extends Thread {
  // Counts down each of the clients completing
  private final CountDownLatch completeLatch;

  // Stores the measurements for the run
  private final Measurements measurements;

  // Whether or not to track the JVM stats per run
  private final boolean trackJVMStats;

  // The clients that are running.
  private final List<ClientThread> clients;

  private final String label;
  private final boolean standardstatus;

  // The interval for reporting status.
  private long sleeptimeNs;

  // JVM max/mins
  private int maxThreads;
  private int minThreads = Integer.MAX_VALUE;
  private long maxUsedMem;
  private long minUsedMem = Long.MAX_VALUE;
  private double maxLoadAvg;
  private double minLoadAvg = Double.MAX_VALUE;
  private long lastGCCount = 0;
  private long lastGCTime = 0;

  // Exit immediately if any FAILED count is non-zero on the status line.
  // Enable with JVM flag: -Dycsb.exit.on.failure=true
  private final boolean exitOnFailure = Boolean.parseBoolean(
      System.getProperty("ycsb.exit.on.failure", "false"));

  // Match lines like: [READ-FAILED: Count=32, ...] (any op, non-zero count)
  private static final Pattern FAIL_RE = Pattern.compile(
      "\\[(READ|UPDATE|INSERT|SCAN)-FAILED:\\s*Count=([1-9][0-9]*)");

  private static final String PROP_THROTTLE_SCHEDULE = "throttle.schedule";
  private static final String PROP_THROTTLE_SCHEDULE_T0MS = "throttle.schedule.t0ms";
  private static final String PROP_THROTTLE_SCHEDULE_BASE_TARGET = "throttle.schedule.base_target";

  private final ThroughputSchedule schedule;
  private final Long scheduleT0Ms;
  private final Double baseTargetTps;

/**
  * Creates a new StatusThread without JVM stat tracking.
  *
  * @param completeLatch         The latch that each client thread will {@link CountDownLatch#countDown()}
  *                              as they complete.
  * @param clients               The clients to collect metrics from.
  * @param label                 The label for the status.
  * @param standardstatus        If true the status is printed to stdout in addition to stderr.
  * @param statusIntervalSeconds The number of seconds between status updates.
  */
  public StatusThread(CountDownLatch completeLatch, List<ClientThread> clients,
                      String label, boolean standardstatus, int statusIntervalSeconds) {
    this(completeLatch, clients, label, standardstatus, statusIntervalSeconds, false, null);
  }

  /**
   * Creates a new StatusThread.
   *
   * @param completeLatch         The latch that each client thread will {@link CountDownLatch#countDown()}
   *                              as they complete.
   * @param clients               The clients to collect metrics from.
   * @param label                 The label for the status.
   * @param standardstatus        If true the status is printed to stdout in addition to stderr.
   * @param statusIntervalSeconds The number of seconds between status updates.
   * @param trackJVMStats         Whether or not to track JVM stats.
   */
  public StatusThread(CountDownLatch completeLatch, List<ClientThread> clients,
                      String label, boolean standardstatus, int statusIntervalSeconds,
                      boolean trackJVMStats) {
    this(completeLatch, clients, label, standardstatus, statusIntervalSeconds, trackJVMStats, null);
  }

  public StatusThread(CountDownLatch completeLatch, List<ClientThread> clients,
                      String label, boolean standardstatus, int statusIntervalSeconds,
                      boolean trackJVMStats, Properties props) {
    this.completeLatch = completeLatch;
    this.clients = clients;
    this.label = label;
    this.standardstatus = standardstatus;
    this.sleeptimeNs = TimeUnit.SECONDS.toNanos(statusIntervalSeconds);
    this.measurements = Measurements.getMeasurements();
    this.trackJVMStats = trackJVMStats;

    if (props != null) {
      ThroughputSchedule sched = null;
      Long t0 = null;
      Double base = null;
      String spec = props.getProperty(PROP_THROTTLE_SCHEDULE);
      if (spec != null && !spec.trim().isEmpty()) {
        try {
          sched = ThroughputSchedule.parse(spec);
        } catch (IllegalArgumentException iae) {
          // ignore bad schedule; status thread keeps running
        }
      }
      String t0s = props.getProperty(PROP_THROTTLE_SCHEDULE_T0MS);
      if (t0s != null) {
        try {
          t0 = Long.parseLong(t0s);
        } catch (NumberFormatException ignore) {
          // ignore parse error
        }
      }
      String baseStr = props.getProperty(PROP_THROTTLE_SCHEDULE_BASE_TARGET);
      if (baseStr != null) {
        try {
          int b = Integer.parseInt(baseStr);
          if (b > 0) {
            base = (double) b;
          }
        } catch (NumberFormatException ignore) {
          // ignore parse error
        }
      }
      this.schedule = sched;
      this.scheduleT0Ms = t0;
      this.baseTargetTps = base;
    } else {
      this.schedule = null;
      this.scheduleT0Ms = null;
      this.baseTargetTps = null;
    }
  }

  /**
   * Run and periodically report status.
   */
  @Override
  public void run() {
    final long startTimeMs = System.currentTimeMillis();
    final long startTimeNanos = System.nanoTime();
    long deadline = startTimeNanos + sleeptimeNs;
    long startIntervalMs = startTimeMs;
    long lastTotalOps = 0;

    boolean alldone;
    
    do {
      long nowMs = System.currentTimeMillis();

      lastTotalOps = computeStats(startTimeMs, startIntervalMs, nowMs, lastTotalOps);

      if (trackJVMStats) {
        measureJVM();
      }

      alldone = waitForClientsUntil(deadline);

      startIntervalMs = nowMs;
      deadline += sleeptimeNs;
    } while (!alldone);

    if (trackJVMStats) {
      measureJVM();
    }
    // Print the final stats.
    computeStats(startTimeMs, startIntervalMs, System.currentTimeMillis(), lastTotalOps);
  }

  /**
   * Computes and prints the stats.
   *
   * @param startTimeMs     The start time of the test.
   * @param startIntervalMs The start time of this interval.
   * @param endIntervalMs   The end time (now) for the interval.
   * @param lastTotalOps    The last total operations count.
   * @return The current operation count.
   */
  private long computeStats(final long startTimeMs, long startIntervalMs, long endIntervalMs,
                            long lastTotalOps) {
    SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS");

    long totalops = 0;
    long todoops = 0;

    // Calculate the total number of operations completed.
    for (ClientThread t : clients) {
      totalops += t.getOpsDone();
      todoops += t.getOpsTodo();
    }
    long interval = endIntervalMs - startTimeMs;
    double throughput = 1000.0 * (totalops / (double) interval);
    double curthroughput = 1000.0 * ((totalops - lastTotalOps)
        / (double) (endIntervalMs - startIntervalMs));
    long estremaining = (long) Math.ceil(todoops / Math.max(throughput, 1e-9));

    DecimalFormat d = new DecimalFormat("#.##");
    String labelString = this.label + format.format(new Date());
    StringBuilder msg = new StringBuilder(labelString)
        .append(" ").append(interval / 1000).append(" sec: ");
    msg.append(totalops).append(" operations; ");
    
    if (totalops != 0) {
      msg.append(d.format(curthroughput)).append(" current ops/sec; ");
    }
    if (todoops != 0) {
      msg.append("est completion in ").append(RemainingFormatter.format(estremaining));
    }
    
    appendThrottleInfo(msg, endIntervalMs);
    msg.append(Measurements.getMeasurements().getSummary());
    
    System.err.println(msg);
    
    if (standardstatus) {
      System.out.println(msg);
    }

    // exit immediately if any failures were reported on this line
    if (exitOnFailure && FAIL_RE.matcher(msg).find()) {
      System.err.println("YCSB: exiting due to operation failures.");
      System.exit(2);
    }
    return totalops;
  }


  private void appendThrottleInfo(StringBuilder msg, long nowMs) {
    if (schedule == null || scheduleT0Ms == null) {
      return;
    }
    long elapsedMs = nowMs - scheduleT0Ms;
    ThroughputSchedule.Slot active = null;
    for (ThroughputSchedule.Slot s : schedule.slots()) {
      if (elapsedMs >= s.getStartMs() && elapsedMs < s.getStopMs()) {
        active = s;
        break;
      }
    }
    if (active != null) {
      msg.append(String.format(" [Throttle: %.1f ops] ", active.getTps()));
    } else {
      if (baseTargetTps != null) {
        msg.append(String.format(" [Throttle: %.1f ops] ", baseTargetTps));
      } else {
        msg.append(" [unthrottled] ");
      }
    }
  }

  /**
   * Waits for all of the client to finish or the deadline to expire.
   *
   * @param deadline The current deadline.
   * @return True if all of the clients completed.
   */
  private boolean waitForClientsUntil(long deadline) {
    boolean alldone = false;
    long now = System.nanoTime();

    while (!alldone && now < deadline) {
      try {
        alldone = completeLatch.await(deadline - now, TimeUnit.NANOSECONDS);
      } catch (InterruptedException ie) {
        // If we are interrupted the thread is being asked to shutdown.
        // Return true to indicate that and reset the interrupt state
        // of the thread.
        Thread.currentThread().interrupt();
        alldone = true;
      }
      now = System.nanoTime();
    }

    return alldone;
  }

  /**
   * Executes the JVM measurements.
   */
  private void measureJVM() {
    final int threads = Utils.getActiveThreadCount();
    if (threads < minThreads) {
      minThreads = threads;
    }
    if (threads > maxThreads) {
      maxThreads = threads;
    }
    measurements.measure("THREAD_COUNT", threads);

    // TODO - once measurements allow for other number types, switch to using
    // the raw bytes. Otherwise we can track in MB to avoid negative values
    // when faced with huge heaps.
    final int usedMem = Utils.getUsedMemoryMegaBytes();
    if (usedMem < minUsedMem) {
      minUsedMem = usedMem;
    }
    if (usedMem > maxUsedMem) {
      maxUsedMem = usedMem;
    }
    measurements.measure("USED_MEM_MB", usedMem);

    // Some JVMs may not implement this feature so if the value is less than
    // zero, just ommit it.
    final double systemLoad = Utils.getSystemLoadAverage();
    if (systemLoad >= 0) {
      measurements.measure("SYS_LOAD_AVG", (int) systemLoad);
      if (systemLoad > maxLoadAvg) {
        maxLoadAvg = systemLoad;
      }
      if (systemLoad < minLoadAvg) {
        minLoadAvg = systemLoad;
      }
    }

    final long gcs = Utils.getGCTotalCollectionCount();
    measurements.measure("GCS", (int) (gcs - lastGCCount));
    final long gcTime = Utils.getGCTotalTime();
    measurements.measure("GCS_TIME", (int) (gcTime - lastGCTime));
    lastGCCount = gcs;
    lastGCTime = gcTime;
  }

  /**
   * @return The maximum threads running during the test.
   */
  public int getMaxThreads() {
    return maxThreads;
  }

  /**
   * @return The minimum threads running during the test.
   */
  public int getMinThreads() {
    return minThreads;
  }

  /**
   * @return The maximum memory used during the test.
   */
  public long getMaxUsedMem() {
    return maxUsedMem;
  }

  /**
   * @return The minimum memory used during the test.
   */
  public long getMinUsedMem() {
    return minUsedMem;
  }

  /**
   * @return The maximum load average during the test.
   */
  public double getMaxLoadAvg() {
    return maxLoadAvg;
  }

  /**
   * @return The minimum load average during the test.
   */
  public double getMinLoadAvg() {
    return minLoadAvg;
  }

  /**
   * @return Whether or not the thread is tracking JVM stats.
   */
  public boolean trackJVMStats() {
    return trackJVMStats;
  }
}
