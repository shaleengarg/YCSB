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

import site.ycsb.measurements.Measurements;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.locks.LockSupport;

/**
 * A thread for executing transactions or data inserts to the database.
 */
public class ClientThread implements Runnable {
  // Counts down each of the clients completing.
  private final CountDownLatch completeLatch;

  private static boolean spinSleep;
  private DB db;
  private boolean dotransactions;
  private Workload workload;
  private long opcount;

  /** Legacy constant-rate fields (preserved for backward compatibility). */
  private double targetOpsPerMs; // legacy per-thread permits per ms (<=0 means unthrottled)
  private long targetOpsTickNs;  // legacy fixed spacing between ops

  private long opsdone;
  private int threadid;
  private int threadcount;
  private Object workloadstate;
  private Properties props;
  private final Measurements measurements;

  /** Dynamic schedule-based throttling (new). */
  private ThroughputSchedule schedule; // parsed from props if provided
  private Long scheduleT0Ms = null;    // run start (set by Client)
  private Double baseTargetTps = null; // total base -target tps outside windows; null => unthrottled
  private int threadCountForRate = 1;  // used to divide total TPS among threads
  private long nextOpDeadlineNs = -1L; // rolling deadline for next op when throttling is active

  // Prop keys (kept in sync with Client.java)
  private static final String PROP_THROTTLE_SCHEDULE = "throttle.schedule";
  private static final String PROP_THROTTLE_SCHEDULE_T0MS = "throttle.schedule.t0ms";
  private static final String PROP_THROTTLE_SCHEDULE_BASE_TARGET = "throttle.schedule.base_target";

  /**
   * Constructor.
   *
   * @param db                   the DB implementation to use
   * @param dotransactions       true to do transactions, false to insert data
   * @param workload             the workload to use
   * @param props                the properties defining the experiment
   * @param opcount              the number of operations (transactions or inserts) to do
   * @param targetperthreadperms target number of operations per thread per ms
   * @param completeLatch        The latch tracking the completion of all clients.
   */
  public ClientThread(DB db, boolean dotransactions, Workload workload, Properties props, long opcount,
                      double targetperthreadperms, CountDownLatch completeLatch) {
    this.db = db;
    this.dotransactions = dotransactions;
    this.workload = workload;
    this.opcount = opcount;
    this.opsdone = 0L;

    // Legacy constant rate setup.
    if (targetperthreadperms > 0) {
      targetOpsPerMs = targetperthreadperms;
      targetOpsTickNs = (long) (1_000_000.0 / targetOpsPerMs); // ns per op for this thread
    } else {
      targetOpsPerMs = -1.0;
      targetOpsTickNs = 0L;
    }

    this.props = props;
    this.measurements = Measurements.getMeasurements();
    spinSleep = Boolean.valueOf(this.props.getProperty("spin.sleep", "false"));
    this.completeLatch = completeLatch;

    // ----- New: schedule-based throttling wiring -----
    final String schedSpec = props.getProperty(PROP_THROTTLE_SCHEDULE);
    if (schedSpec != null && !schedSpec.trim().isEmpty()) {
      try {
        this.schedule = ThroughputSchedule.parse(schedSpec);
      } catch (IllegalArgumentException iae) {
        // Fail fast to make issues obvious to the user.
        throw iae;
      }
    }

    final String t0Str = props.getProperty(PROP_THROTTLE_SCHEDULE_T0MS);
    if (t0Str != null) {
      try {
        this.scheduleT0Ms = Long.parseLong(t0Str);
      } catch (NumberFormatException ignore) {
        // Ignored: invalid t0; schedule timing will be treated as absent.
      }
    }

    final String baseStr = props.getProperty(PROP_THROTTLE_SCHEDULE_BASE_TARGET);
    if (baseStr != null) {
      try {
        int base = Integer.parseInt(baseStr);
        if (base > 0) {
          this.baseTargetTps = (double) base;
        }
      } catch (NumberFormatException ignore) {
        // Ignored: invalid base target; outside windows will be unthrottled.
      }
    }
  } // end constructor

  public void setThreadId(final int threadId) {
    this.threadid = threadId;
  }

  public void setThreadCount(final int threadCount) {
    this.threadcount = threadCount;
    this.threadCountForRate = Math.max(1, threadCount);
  }

  public long getOpsDone() {
    return opsdone;
  }

  @Override
  public void run() {
    try {
      db.init();
    } catch (DBException e) {
      e.printStackTrace();
      e.printStackTrace(System.out);
      return;
    }

    try {
      workloadstate = workload.initThread(props, threadid, threadcount);
    } catch (WorkloadException e) {
      e.printStackTrace();
      e.printStackTrace(System.out);
      return;
    }


    // NOTE: We use nanoTime + parkNanos consistently with Measurements.

    // Initial jitter so threads don't stampede the DB at t=0.
    // Use dynamic rate if schedule is active; otherwise legacy.
    double initialPermitsPerMs = currentPerThreadPermitsPerMs();
    if ((initialPermitsPerMs > 0) && (initialPermitsPerMs <= 1.0)) {
      long tickNs = (long) (1_000_000.0 / initialPermitsPerMs);
      long randomMinorDelay = ThreadLocalRandom.current().nextInt((int) Math.max(1, tickNs));
      sleepUntil(System.nanoTime() + randomMinorDelay);
    }

    try {
      if (dotransactions) {
        while (((opcount == 0) || (opsdone < opcount)) && !workload.isStopRequested()) {
          if (!workload.doTransaction(db, workloadstate)) {
            break;
          }
          opsdone++;
          throttle();
        }
      } else {
        while (((opcount == 0) || (opsdone < opcount)) && !workload.isStopRequested()) {
          if (!workload.doInsert(db, workloadstate)) {
            break;
          }
          opsdone++;
          throttle();
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
      e.printStackTrace(System.out);
      System.exit(0);
    }

    try {
      measurements.setIntendedStartTimeNs(0);
      db.cleanup();
    } catch (DBException e) {
      e.printStackTrace();
      e.printStackTrace(System.out);
    } finally {
      completeLatch.countDown();
    }
  }

  /**
   * Sleep helper that optionally spins for very fine waits.
   */
  private static void sleepUntil(long deadline) {
    while (System.nanoTime() < deadline) {
      if (!spinSleep) {
        long remain = deadline - System.nanoTime();
        if (remain > 0) {
          LockSupport.parkNanos(remain);
        }
      }
    }
  }

  /**
   * Compute the current per-thread permits/ms honoring the schedule if present.
   * Returns a negative value to indicate "unthrottled".
   */
  private double currentPerThreadPermitsPerMs() {
    // If no schedule or missing t0, fall back to legacy constant rate.
    if (this.schedule == null || this.scheduleT0Ms == null) {
      return targetOpsPerMs; // negative or >0 as originally configured
    }

    long nowMs = System.currentTimeMillis();
    long elapsedMs = nowMs - this.scheduleT0Ms;

    // Total TPS in effect at 'now'.
    // If outside windows and baseTargetTps is null => unthrottled.
    Double defaultTps = this.baseTargetTps; // may be null
    double effectiveTps = this.schedule.tpsForElapsedMs(elapsedMs, defaultTps);

    if (Double.isInfinite(effectiveTps)) {
      return -1.0; // unthrottled
    }

    // Convert total TPS -> per-thread permits/ms
    double perThreadTps = effectiveTps / (double) this.threadCountForRate;
    return perThreadTps / 1000.0;
  }

  /**
   * Throttle based on either dynamic schedule or legacy constant rate.
   * Uses a rolling nextOpDeadlineNs so we can adjust tick size per operation.
   */
  private void throttle() {
    double permitsPerMs = currentPerThreadPermitsPerMs();

    // Unthrottled?
    if (!(permitsPerMs > 0)) {
      // Reset rolling state so that if throttling becomes active later
      // we don't accumulate huge sleep.
      nextOpDeadlineNs = -1L;
      return;
    }

    // Compute tick for current op:
    // tickNs = 1e9 / (permitsPerMs * 1000) = 1e6 / permitsPerMs
    long tickNs = (long) Math.max(1L, Math.floor(1_000_000.0 / permitsPerMs));

    // Initialize or advance the per-op deadline.
    long now = System.nanoTime();
    if (nextOpDeadlineNs <= 0) {
      nextOpDeadlineNs = now + tickNs;
    } else {
      nextOpDeadlineNs += tickNs;
      // If we're far behind (e.g., due to GC), avoid oversleeping forever:
      // catch up to now + small tick.
      if (nextOpDeadlineNs < now) {
        long cap = Math.min(tickNs, 5_000_000L); // cap catch-up to 5ms to smooth spikes
        nextOpDeadlineNs = now + cap;
      }
    }

    // Sleep until deadline and record intended start time for measurements.
    sleepUntil(nextOpDeadlineNs);
    measurements.setIntendedStartTimeNs(nextOpDeadlineNs);
  }

  /**
   * The total amount of work this thread is still expected to do.
   */
  long getOpsTodo() {
    long todo = opcount - opsdone;
    return todo < 0 ? 0 : todo;
  }
}
