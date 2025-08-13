/**
 * Copyright (c) 2010-2016 Yahoo! Inc., 2017 YCSB contributors All rights reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
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
import site.ycsb.measurements.exporter.MeasurementsExporter;
import site.ycsb.measurements.exporter.TextMeasurementsExporter;
import org.apache.htrace.core.HTraceConfiguration;
import org.apache.htrace.core.TraceScope;
import org.apache.htrace.core.Tracer;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Turn seconds remaining into more useful units.
 * i.e. if there are hours or days worth of seconds, use them.
 */
final class RemainingFormatter {
  private RemainingFormatter() {
    // not used
  }

  public static StringBuilder format(long seconds) {
    StringBuilder time = new StringBuilder();
    long days = TimeUnit.SECONDS.toDays(seconds);
    if (days > 0) {
      time.append(days).append(days == 1 ? " day " : " days ");
      seconds -= TimeUnit.DAYS.toSeconds(days);
    }
    long hours = TimeUnit.SECONDS.toHours(seconds);
    if (hours > 0) {
      time.append(hours).append(hours == 1 ? " hour " : " hours ");
      seconds -= TimeUnit.HOURS.toSeconds(hours);
    }
    /* Only include minute granularity if we're < 1 day. */
    if (days < 1) {
      long minutes = TimeUnit.SECONDS.toMinutes(seconds);
      if (minutes > 0) {
        time.append(minutes).append(minutes == 1 ? " minute " : " minutes ");
        // subtract minutes (not seconds)
        seconds -= TimeUnit.MINUTES.toSeconds(minutes);
      }
    }
    /* Only bother to include seconds if we're < 1 minute */
    if (time.length() == 0) {
      time.append(seconds).append(seconds == 1 ? " second " : " seconds ");
    }
    return time;
  }
}

/**
 * Main class for executing YCSB.
 */
public final class Client {
  private Client() {
    //not used
  }

  public static final String DEFAULT_RECORD_COUNT = "0";

  /**
   * The target number of operations to perform.
   */
  public static final String OPERATION_COUNT_PROPERTY = "operationcount";

  /**
   * The number of records to load into the database initially.
   */
  public static final String RECORD_COUNT_PROPERTY = "recordcount";

  /**
   * The workload class to be loaded.
   */
  public static final String WORKLOAD_PROPERTY = "workload";

  /**
   * The database class to be used.
   */
  public static final String DB_PROPERTY = "db";

  /**
   * The exporter class to be used. The default is
   * site.ycsb.measurements.exporter.TextMeasurementsExporter.
   */
  public static final String EXPORTER_PROPERTY = "exporter";

  /**
   * If set, results are written to this file instead of STDOUT.
   */
  public static final String EXPORT_FILE_PROPERTY = "exportfile";

  /**
   * The number of YCSB client threads to run.
   */
  public static final String THREAD_COUNT_PROPERTY = "threadcount";

  /**
   * Indicates how many inserts to do if less than recordcount.
   * Useful for partitioning the load among multiple servers if the client is the bottleneck.
   * Additionally workloads should support the "insertstart" property which tells them which record to start at.
   * Workloads should also support "insertstart".
   */
  public static final String INSERT_COUNT_PROPERTY = "insertcount";

  /**
   * Target number of operations per second.
   */
  public static final String TARGET_PROPERTY = "target";

  /**
   * The maximum amount of time (in seconds) for which the benchmark will be run.
   */
  public static final String MAX_EXECUTION_TIME = "maxexecutiontime";

  /**
   * Whether or not this is the transaction phase (run) or not (load).
   */
  public static final String DO_TRANSACTIONS_PROPERTY = "dotransactions";

  /**
   * Whether or not to show status during run.
   */
  public static final String STATUS_PROPERTY = "status";

  /**
   * Use label for status (e.g. to label one experiment out of a whole batch).
   */
  public static final String LABEL_PROPERTY = "label";

  /**
   * Specifies a schedule for throttling the client throughput.
   * Format: start:stop:tps[,start:stop:tps...], times relative to run start.
   * Units: ms|s|m|h (default s). Outside windows, base -target is honored,
   * or unthrottled if -target is not set.
   */
  private static final String PROP_THROTTLE_SCHEDULE = "throttle.schedule";

  /** Props populated for threads when a schedule is used. */
  private static final String PROP_THROTTLE_SCHEDULE_T0MS = "throttle.schedule.t0ms";
  private static final String PROP_THROTTLE_SCHEDULE_BASE_TARGET = "throttle.schedule.base_target";

  /** Optional thread for progress/JVM stats. */
  private static StatusThread statusthread = null;

  // HTrace integration
  private static final String HTRACE_KEY_PREFIX = "htrace.";
  private static final String CLIENT_WORKLOAD_INIT_SPAN = "Client#workload_init";
  private static final String CLIENT_INIT_SPAN = "Client#init";
  private static final String CLIENT_WORKLOAD_SPAN = "Client#workload";
  private static final String CLIENT_CLEANUP_SPAN = "Client#cleanup";
  private static final String CLIENT_EXPORT_MEASUREMENTS_SPAN = "Client#export_measurements";

  public static void usageMessage() {
    System.out.println("Usage: java site.ycsb.Client [options]");
    System.out.println("Options:");
    System.out.println("  -threads n: execute using n threads (default: 1)");
    System.out.println("      (or -p threadcount=n)");
    System.out.println("  -target n: attempt n operations per second (default: unlimited)");
    System.out.println("      (or -p target=n)");
    System.out.println("  -load: run the loading phase of the workload");
    System.out.println("  -t: run the transactions phase of the workload (default)");
    System.out.println("  -db dbname: DB class (default: site.ycsb.BasicDB)");
    System.out.println("      (or -p db=...)");
    System.out.println("  -P file: load properties from file (can be given multiple times)");
    System.out.println("  -p name=value: set a property (overrides property files)");
    System.out.println("  -s: show status during run");
    System.out.println("  -l label: label for status output");
    System.out.println();
    System.out.println("Required properties:");
    System.out.println("  " + WORKLOAD_PROPERTY + ": workload class (e.g. site.ycsb.workloads.CoreWorkload)");
    System.out.println();
    System.out.println("Optional throttle schedule (overrides -target during windows):");
    System.out.println("  -p " + PROP_THROTTLE_SCHEDULE + "=start:stop:tps[,start:stop:tps...]");
    System.out.println("     start/stop relative to run start; units: ms|s|m|h (default s).");
    System.out.println("     Example: -p " + PROP_THROTTLE_SCHEDULE + "=0s:2m:1500,150s:5m:800");
    System.out.println();
    System.out.println("To run transactions from multiple servers, start one client on each.");
    System.out.println("To run load from multiple servers, also use insertcount/insertstart to partition keys.");
  }

  public static boolean checkRequiredProperties(Properties props) {
    if (props.getProperty(WORKLOAD_PROPERTY) == null) {
      System.out.println("Missing property: " + WORKLOAD_PROPERTY);
      return false;
    }
    return true;
  }

  /**
   * Exports the measurements to either sysout or a file using the exporter
   * loaded from conf.
   *
   * @throws IOException Either failed to write to output stream or failed to close it.
   */
  private static void exportMeasurements(Properties props, long opcount, long runtime)
      throws IOException {
    MeasurementsExporter exporter = null;
    try {
      // if no destination file is provided the results will be written to stdout
      OutputStream out;
      String exportFile = props.getProperty(EXPORT_FILE_PROPERTY);
      if (exportFile == null) {
        out = System.out;
      } else {
        out = new FileOutputStream(exportFile);
      }

      String exporterStr = props.getProperty(
          EXPORTER_PROPERTY,
          "site.ycsb.measurements.exporter.TextMeasurementsExporter");
      try {
        exporter = (MeasurementsExporter) Class.forName(exporterStr)
            .getConstructor(OutputStream.class)
            .newInstance(out);
      } catch (Exception e) {
        System.err.println("Could not find exporter " + exporterStr
            + ", will use default text reporter.");
        e.printStackTrace();
        exporter = new TextMeasurementsExporter(out);
      }

      exporter.write("OVERALL", "RunTime(ms)", runtime);
      double throughput = 1000.0 * (opcount) / (runtime);
      exporter.write("OVERALL", "Throughput(ops/sec)", throughput);

      final Map<String, Long[]> gcs = Utils.getGCStatst();
      long totalGCCount = 0;
      long totalGCTime = 0;
      for (final Entry<String, Long[]> entry : gcs.entrySet()) {
        exporter.write("TOTAL_GCS_" + entry.getKey(), "Count", entry.getValue()[0]);
        exporter.write("TOTAL_GC_TIME_" + entry.getKey(), "Time(ms)", entry.getValue()[1]);
        exporter.write("TOTAL_GC_TIME_%_" + entry.getKey(), "Time(%)",
            ((double) entry.getValue()[1] / runtime) * (double) 100);
        totalGCCount += entry.getValue()[0];
        totalGCTime += entry.getValue()[1];
      }
      exporter.write("TOTAL_GCs", "Count", totalGCCount);

      exporter.write("TOTAL_GC_TIME", "Time(ms)", totalGCTime);
      exporter.write("TOTAL_GC_TIME_%", "Time(%)",
          ((double) totalGCTime / runtime) * (double) 100);
      if (statusthread != null && statusthread.trackJVMStats()) {
        exporter.write("MAX_MEM_USED", "MBs", statusthread.getMaxUsedMem());
        exporter.write("MIN_MEM_USED", "MBs", statusthread.getMinUsedMem());
        exporter.write("MAX_THREADS", "Count", statusthread.getMaxThreads());
        exporter.write("MIN_THREADS", "Count", statusthread.getMinThreads());
        exporter.write("MAX_SYS_LOAD_AVG", "Load", statusthread.getMaxLoadAvg());
        exporter.write("MIN_SYS_LOAD_AVG", "Load", statusthread.getMinLoadAvg());
      }

      Measurements.getMeasurements().exportMeasurements(exporter);
    } finally {
      if (exporter != null) {
        exporter.close();
      }
    }
  }

  @SuppressWarnings("unchecked")
  public static void main(String[] args) {
    Properties props = parseArguments(args);

    boolean status = Boolean.valueOf(props.getProperty(STATUS_PROPERTY, String.valueOf(false)));
    String label = props.getProperty(LABEL_PROPERTY, "");
    long maxExecutionTime = Integer.parseInt(props.getProperty(MAX_EXECUTION_TIME, "0"));

    // threadcount, db, -target
    int threadcount = Integer.parseInt(props.getProperty(THREAD_COUNT_PROPERTY, "1"));
    String dbname = props.getProperty(DB_PROPERTY, "site.ycsb.BasicDB");
    int target = Integer.parseInt(props.getProperty(TARGET_PROPERTY, "0"));

    // Parse schedule and validate vs maxexecutiontime (if provided).
    final String scheduleSpec = props.getProperty(PROP_THROTTLE_SCHEDULE);
    final ThroughputSchedule schedule = ThroughputSchedule.parse(scheduleSpec);
    validateThrottleSchedule(schedule, maxExecutionTime);

    // Base target per-thread per-ms (legacy pacing value).
    double targetperthreadperms = computeLegacyPerThreadPermitsMs(target, threadcount);

    Thread warningthread = setupWarningThread();
    warningthread.start();

    Measurements.setProperties(props);

    Workload workload = getWorkload(props);
    final Tracer tracer = getTracer(props, workload);
    initWorkload(props, warningthread, workload, tracer);

    // If schedule is present, stash base target and t0 in props for clients BEFORE constructing threads.
    if (schedule != null) {
      props.setProperty(PROP_THROTTLE_SCHEDULE_BASE_TARGET, Integer.toString(Math.max(target, 0)));
      props.setProperty(PROP_THROTTLE_SCHEDULE_T0MS, Long.toString(System.currentTimeMillis()));
      logThrottleSchedule(schedule, target);
    }

    System.err.println("Starting test.");
    final CountDownLatch completeLatch = new CountDownLatch(threadcount);

    final List<ClientThread> clients = initDb(
        dbname, props, threadcount, targetperthreadperms, workload, tracer, completeLatch);

    if (status) {
      startStatusThread(completeLatch, clients, label, props);
    }

    // Start and (optionally) terminate after maxExecutionTime.
    long st;
    long en;
    int opsDone;
    Thread terminator = null;

    try (final TraceScope span = tracer.newScope(CLIENT_WORKLOAD_SPAN)) {
      final Map<Thread, ClientThread> threads = startClientThreads(clients, tracer);

      // Measure wall time for the run (separate from t0 used for throttling).
      st = System.currentTimeMillis();

      if (maxExecutionTime > 0) {
        terminator = new TerminatorThread(maxExecutionTime, threads.keySet(), workload);
        terminator.start();
      }

      opsDone = joinClientsAndCountOps(threads);
      en = System.currentTimeMillis();
    }

    // Pass runtime to keep parameter count within Checkstyle limit.
    cleanupAndExport(props, status, workload, tracer, terminator, en - st, opsDone);
    System.exit(0);
  }

  // ------------------------ helpers to keep main short ------------------------

  private static void validateThrottleSchedule(ThroughputSchedule schedule, long maxExecutionTime) {
    if (schedule == null) {
      return;
    }
    if (maxExecutionTime > 0) {
      long maxMs = maxExecutionTime * 1000L;
      for (ThroughputSchedule.Slot s : schedule.slots()) {
        if (s.getStopMs() > maxMs) {
          throw new IllegalArgumentException(
              "throttle.schedule STOP (" + s.getStopMs()
                  + " ms) exceeds maxexecutiontime (" + maxMs + " ms): " + s);
        }
      }
    } else {
      System.err.println("[YCSB] Warning: throttle.schedule supplied without maxexecutiontime; "
          + "cannot verify that STOP < total runtime.");
    }
  }

  private static double computeLegacyPerThreadPermitsMs(int target, int threadcount) {
    if (target <= 0) {
      return -1.0;
    }
    double targetperthread = ((double) target) / ((double) threadcount);
    return targetperthread / 1000.0;
  }

  private static void logThrottleSchedule(ThroughputSchedule schedule, int baseTarget) {
    StringBuilder sb = new StringBuilder();
    sb.append("[YCSB] throttle.schedule: ")
        .append(schedule.slots().size())
        .append(" window(s)\n");
    for (ThroughputSchedule.Slot s : schedule.slots()) {
      sb.append(String.format(Locale.ROOT,
          "  %d–%d ms -> %.3f tps%n", s.getStartMs(), s.getStopMs(), s.getTps()));
    }
    if (baseTarget > 0) {
      sb.append("Outside windows: honoring base -target = ").append(baseTarget).append(" tps\n");
    } else {
      sb.append("Outside windows: unthrottled (no -target)\n");
    }
    System.err.print(sb.toString());
  }

  private static Map<Thread, ClientThread> startClientThreads(List<ClientThread> clients, Tracer tracer) {
    final Map<Thread, ClientThread> threads = new HashMap<Thread, ClientThread>(clients.size());
    for (ClientThread client : clients) {
      Thread t = new Thread(tracer.wrap(client, "ClientThread"));
      threads.put(t, client);
      t.start();
    }
    return threads;
  }

  private static int joinClientsAndCountOps(Map<Thread, ClientThread> threads) {
    int opsDone = 0;
    for (Map.Entry<Thread, ClientThread> entry : threads.entrySet()) {
      try {
        entry.getKey().join();
        opsDone += entry.getValue().getOpsDone();
      } catch (InterruptedException ignored) {
        // ignored
      }
    }
    return opsDone;
  }

  private static void cleanupAndExport(Properties props, boolean status, Workload workload,
                                       Tracer tracer, Thread terminator,
                                       long runtimeMs, int opsDone) {
    try {
      try (final TraceScope span = tracer.newScope(CLIENT_CLEANUP_SPAN)) {

        if (terminator != null && !terminator.isInterrupted()) {
          terminator.interrupt();
        }

        if (status) {
          // wake up status thread if it's asleep
          statusthread.interrupt();
          // at this point we assume all the monitored threads are already gone as per above join loop.
          try {
            statusthread.join();
          } catch (InterruptedException ignored) {
            // ignored
          }
        }

        workload.cleanup();
      }
    } catch (WorkloadException e) {
      e.printStackTrace();
      e.printStackTrace(System.out);
      System.exit(0);
    }

    try {
      try (final TraceScope span = tracer.newScope(CLIENT_EXPORT_MEASUREMENTS_SPAN)) {
        exportMeasurements(props, opsDone, runtimeMs);
      }
    } catch (IOException e) {
      System.err.println("Could not export measurements, error: " + e.getMessage());
      e.printStackTrace();
      System.exit(-1);
    }
  }

  private static void startStatusThread(CountDownLatch completeLatch, List<ClientThread> clients,
                                        String label, Properties props) {
    boolean standardstatus = false;
    if (props.getProperty(Measurements.MEASUREMENT_TYPE_PROPERTY, "")
        .compareTo("timeseries") == 0) {
      standardstatus = true;
    }
    int statusIntervalSeconds = Integer.parseInt(props.getProperty("status.interval", "10"));
    boolean trackJVMStats = props.getProperty(
        Measurements.MEASUREMENT_TRACK_JVM_PROPERTY,
        Measurements.MEASUREMENT_TRACK_JVM_PROPERTY_DEFAULT).equals("true");

    statusthread = new StatusThread(
        completeLatch, clients, label, standardstatus, statusIntervalSeconds, trackJVMStats, props);
    statusthread.start();
  }

  private static List<ClientThread> initDb(String dbname, Properties props, int threadcount,
                                           double targetperthreadperms, Workload workload, Tracer tracer,
                                           CountDownLatch completeLatch) {
    boolean initFailed = false;
    boolean dotransactions =
        Boolean.valueOf(props.getProperty(DO_TRANSACTIONS_PROPERTY, String.valueOf(true)));

    final List<ClientThread> clients = new ArrayList<ClientThread>(threadcount);
    try (final TraceScope span = tracer.newScope(CLIENT_INIT_SPAN)) {
      long opcount;
      if (dotransactions) {
        opcount = Long.parseLong(props.getProperty(OPERATION_COUNT_PROPERTY, "0"));
      } else {
        if (props.containsKey(INSERT_COUNT_PROPERTY)) {
          opcount = Long.parseLong(props.getProperty(INSERT_COUNT_PROPERTY, "0"));
        } else {
          opcount = Long.parseLong(props.getProperty(RECORD_COUNT_PROPERTY, DEFAULT_RECORD_COUNT));
        }
      }
      if (threadcount > opcount && opcount > 0) {
        threadcount = (int) (opcount);
        System.out.println(
            "Warning: the threadcount is bigger than recordcount, the threadcount will be recordcount!");
      }
      for (int threadid = 0; threadid < threadcount; threadid++) {
        DB db;
        try {
          db = DBFactory.newDB(dbname, props, tracer);
        } catch (UnknownDBException e) {
          System.out.println("Unknown DB " + dbname);
          initFailed = true;
          break;
        }

        long threadopcount = opcount / threadcount;

        // ensure correct number of operations if opcount not multiple of threadcount
        if (threadid < opcount % threadcount) {
          ++threadopcount;
        }

        ClientThread t = new ClientThread(
            db, dotransactions, workload, props, threadopcount, targetperthreadperms, completeLatch);
        t.setThreadId(threadid);
        t.setThreadCount(threadcount);
        clients.add(t);
      }

      if (initFailed) {
        System.err.println("Error initializing datastore bindings.");
        System.exit(0);
      }
    }
    return clients;
  }

  private static Tracer getTracer(Properties props, Workload workload) {
    return new Tracer.Builder("YCSB " + workload.getClass().getSimpleName())
        .conf(getHTraceConfiguration(props))
        .build();
  }

  private static void initWorkload(Properties props, Thread warningthread,
                                   Workload workload, Tracer tracer) {
    try {
      try (final TraceScope span = tracer.newScope(CLIENT_WORKLOAD_INIT_SPAN)) {
        workload.init(props);
        warningthread.interrupt();
      }
    } catch (WorkloadException e) {
      e.printStackTrace();
      e.printStackTrace(System.out);
      System.exit(0);
    }
  }

  private static HTraceConfiguration getHTraceConfiguration(Properties props) {
    final Map<String, String> filteredProperties = new HashMap<String, String>();
    for (String key : props.stringPropertyNames()) {
      if (key.startsWith(HTRACE_KEY_PREFIX)) {
        filteredProperties.put(
            key.substring(HTRACE_KEY_PREFIX.length()), props.getProperty(key));
      }
    }
    return HTraceConfiguration.fromMap(filteredProperties);
  }

  private static Thread setupWarningThread() {
    //show a warning message that creating the workload is taking a while
    //but only do so if it is taking longer than 2 seconds
    //(showing the message right away if the setup wasn't taking very long was confusing people)
    return new Thread() {
      @Override
      public void run() {
        try {
          sleep(2000);
        } catch (InterruptedException e) {
          return;
        }
        System.err.println(" (might take a few minutes for large data sets)");
      }
    };
  }

  private static Workload getWorkload(Properties props) {
    ClassLoader classLoader = Client.class.getClassLoader();

    try {
      Properties projectProp = new Properties();
      projectProp.load(classLoader.getResourceAsStream("project.properties"));
      System.err.println("YCSB Client " + projectProp.getProperty("version"));
    } catch (IOException e) {
      System.err.println("Unable to retrieve client version.");
    }

    System.err.println();
    System.err.println("Loading workload...");
    try {
      Class workloadclass = classLoader.loadClass(props.getProperty(WORKLOAD_PROPERTY));
      return (Workload) workloadclass.newInstance();
    } catch (Exception e) {
      e.printStackTrace();
      e.printStackTrace(System.out);
      System.exit(0);
    }
    return null;
  }

  private static Properties parseArguments(String[] args) {
    Properties props = new Properties();
    System.err.print("Command line:");
    for (String arg : args) {
      System.err.print(" " + arg);
    }
    System.err.println();

    Properties fileprops = new Properties();
    int argindex = 0;

    if (args.length == 0) {
      usageMessage();
      System.out.println("At least one argument specifying a workload is required.");
      System.exit(0);
    }

    while (args[argindex].startsWith("-")) {
      if (args[argindex].compareTo("-threads") == 0) {
        argindex++;
        if (argindex >= args.length) {
          usageMessage();
          System.out.println("Missing argument value for -threads.");
          System.exit(0);
        }
        int tcount = Integer.parseInt(args[argindex]);
        props.setProperty(THREAD_COUNT_PROPERTY, String.valueOf(tcount));
        argindex++;
      } else if (args[argindex].compareTo("-target") == 0) {
        argindex++;
        if (argindex >= args.length) {
          usageMessage();
          System.out.println("Missing argument value for -target.");
          System.exit(0);
        }
        int ttarget = Integer.parseInt(args[argindex]);
        props.setProperty(TARGET_PROPERTY, String.valueOf(ttarget));
        argindex++;
      } else if (args[argindex].compareTo("-load") == 0) {
        props.setProperty(DO_TRANSACTIONS_PROPERTY, String.valueOf(false));
        argindex++;
      } else if (args[argindex].compareTo("-t") == 0) {
        props.setProperty(DO_TRANSACTIONS_PROPERTY, String.valueOf(true));
        argindex++;
      } else if (args[argindex].compareTo("-s") == 0) {
        props.setProperty(STATUS_PROPERTY, String.valueOf(true));
        argindex++;
      } else if (args[argindex].compareTo("-db") == 0) {
        argindex++;
        if (argindex >= args.length) {
          usageMessage();
          System.out.println("Missing argument value for -db.");
          System.exit(0);
        }
        props.setProperty(DB_PROPERTY, args[argindex]);
        argindex++;
      } else if (args[argindex].compareTo("-l") == 0) {
        argindex++;
        if (argindex >= args.length) {
          usageMessage();
          System.out.println("Missing argument value for -l.");
          System.exit(0);
        }
        props.setProperty(LABEL_PROPERTY, args[argindex]);
        argindex++;
      } else if (args[argindex].compareTo("-P") == 0) {
        argindex++;
        if (argindex >= args.length) {
          usageMessage();
          System.out.println("Missing argument value for -P.");
          System.exit(0);
        }
        String propfile = args[argindex];
        argindex++;

        Properties myfileprops = new Properties();
        try {
          myfileprops.load(new FileInputStream(propfile));
        } catch (IOException e) {
          System.out.println("Unable to open the properties file " + propfile);
          System.out.println(e.getMessage());
          System.exit(0);
        }

        // remove call to stringPropertyNames to make compilable under Java 1.5
        for (Enumeration e = myfileprops.propertyNames(); e.hasMoreElements();) {
          String prop = (String) e.nextElement();
          fileprops.setProperty(prop, myfileprops.getProperty(prop));
        }

      } else if (args[argindex].compareTo("-p") == 0) {
        argindex++;
        if (argindex >= args.length) {
          usageMessage();
          System.out.println("Missing argument value for -p");
          System.exit(0);
        }
        int eq = args[argindex].indexOf('=');
        if (eq < 0) {
          usageMessage();
          System.out.println("Argument '-p' expected key=value (e.g., -p operationcount=99999)");
          System.exit(0);
        }

        String name = args[argindex].substring(0, eq);
        String value = args[argindex].substring(eq + 1);
        props.put(name, value);
        argindex++;
      } else {
        usageMessage();
        System.out.println("Unknown option " + args[argindex]);
        System.exit(0);
      }

      if (argindex >= args.length) {
        break;
      }
    }

    if (argindex != args.length) {
      usageMessage();
      if (argindex < args.length) {
        System.out.println("Found value without flag (e.g., -p, -s): " + args[argindex]);
      } else {
        System.out.println("Found flag without value at end of arguments.");
      }
      System.exit(0);
    }

    //overwrite file properties with properties from the command line

    //Issue #5 - remove call to stringPropertyNames to make compilable under Java 1.5
    for (Enumeration e = props.propertyNames(); e.hasMoreElements();) {
      String prop = (String) e.nextElement();

      fileprops.setProperty(prop, props.getProperty(prop));
    }

    props = fileprops;

    if (!checkRequiredProperties(props)) {
      System.out.println("Failed check required properties.");
      System.exit(0);
    }

    return props;
  }
}
