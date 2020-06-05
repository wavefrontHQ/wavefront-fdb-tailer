package com.wavefront.integrations;

import com.codahale.metrics.*;
import com.codahale.metrics.graphite.Graphite;
import com.codahale.metrics.graphite.GraphiteReporter;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.AtomicDouble;
import com.wavefront.dropwizard.metrics.DropwizardMetricsReporter;
import com.wavefront.internal.reporter.WavefrontInternalReporter;
import com.wavefront.sdk.common.WavefrontSender;
import com.wavefront.sdk.direct_ingestion.WavefrontDirectIngestionClient;
import com.wavefront.sdk.proxy.WavefrontProxyClient;
import org.apache.commons.io.input.Tailer;

import java.io.File;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.concurrent.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;

/**
 * This class collects and periodically reports metrics via Wavefront Proxy, Wavefront Direct Ingestion, or GraphiteReporter.
 */
public class FDBMetricsReporter {

    private final static Logger logger = Logger.getLogger(FDBMetricsReporter.class.getCanonicalName());

    private final static String SERVICE_NAME = "fdbtailer";

    private final static int FILE_PARSING_PERIOD = 30;

    private final static int METRICS_REPORTING_PERIOD = 60;

    static {
        SharedMetricRegistries.setDefault("defaultFDBMetrics", new MetricRegistry());
    }

    private ScheduledReporter reporter;

    private WavefrontInternalReporter shardedReporter = null;

    private WavefrontSender sender;

    private String directory;

    private String matching;

    private String prefix;

    private LoadingCache<String, AtomicDouble> values;

    private LoadingCache<String, Gauge<Double>> gauges;

    String metricName(String name) {
        return prefix + name;
    }

    public FDBMetricsReporter(FDBMetricsReporterArguments arguments) throws UnknownHostException {
        this.directory = arguments.getDirectory();
        this.matching = arguments.getMatching();
        this.prefix = arguments.getPrefix();

        this.values = CacheBuilder.newBuilder().build(
                new CacheLoader<String, AtomicDouble>() {
                    @Override
                    public AtomicDouble load(String key) {
                        return new AtomicDouble();
                    }
                });

        this.gauges = CacheBuilder.newBuilder().build(
                new CacheLoader<String, Gauge<Double>>() {
                    @Override
                    public Gauge load(final String key) {
                        return SharedMetricRegistries.getDefault().gauge(metricName(key),
                                () -> new Gauge() {
                                    AtomicDouble value = values.getUnchecked(key);

                                    @Override
                                    public Double getValue() {
                                        return value.get();
                                    }
                                });
                    }
                }
        );

        if (arguments.getReporterType() == FDBMetricsReporterArguments.ReporterType.PROXY) {
            initProxy(arguments.getProxyHost(), arguments.getProxyPort());
        } else if (arguments.getReporterType() == FDBMetricsReporterArguments.ReporterType.DIRECT) {
            initDirect(arguments.getServer(), arguments.getToken());
        } else if (arguments.getReporterType() == FDBMetricsReporterArguments.ReporterType.GRAPHITE) {
            initGraphite(arguments.getGraphiteServer(), arguments.getGraphitePort());
        } else if (arguments.getReporterType() == FDBMetricsReporterArguments.ReporterType.SHARDED) {
            initSharded(arguments.getProxyHost(), arguments.getProxyPort());
        }
    }

    private void initDirect(String server, String token) {
        this.sender = new WavefrontDirectIngestionClient.Builder(server, token).build();

        this.reporter = DropwizardMetricsReporter.forRegistry(SharedMetricRegistries.getDefault()).
                withSource(getHostName()).
                withReporterPointTag("service", SERVICE_NAME).
                withJvmMetrics().
                build(this.sender);
    }

    private void initProxy(String proxyHostname, int proxyPort) throws UnknownHostException {
        this.sender = new WavefrontProxyClient.Builder(proxyHostname).metricsPort(proxyPort).build();

        this.reporter = DropwizardMetricsReporter.forRegistry(SharedMetricRegistries.getDefault()).
                withSource(getHostName()).
                withReporterPointTag("service", SERVICE_NAME).
                withJvmMetrics().
                build(this.sender);

    }

    private void initGraphite(String graphiteServer, int graphitePort) {
        final Graphite graphite = new Graphite(new InetSocketAddress(graphiteServer, graphitePort));

        this.reporter = GraphiteReporter.forRegistry(SharedMetricRegistries.getDefault()).
                convertRatesTo(TimeUnit.SECONDS).
                convertDurationsTo(TimeUnit.MILLISECONDS).
                filter(MetricFilter.ALL).
                build(graphite);

    }

    private void initSharded(String proxyHostname, int proxyPort) throws UnknownHostException {
        this.sender = new WavefrontProxyClient.Builder(proxyHostname).metricsPort(proxyPort).build();

        this.shardedReporter = new WavefrontInternalReporter.Builder().
                withSource(getHostName()).
                withReporterPointTag("service", SERVICE_NAME).
                includeJvmMetrics().
                build(this.sender);
    }


    private String getHostName() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            logger.warning("Cannot get host name.  Source will default to Unknown.");
            return "Unknown";
        }
    }

    void start() {
        if (this.shardedReporter != null) {
            this.shardedReporter.start(METRICS_REPORTING_PERIOD, TimeUnit.SECONDS);
        } else {
            this.reporter.start(METRICS_REPORTING_PERIOD, TimeUnit.SECONDS);
        }
        collectMetrics();
    }

    private void collectMetrics() {
        final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
        final ScheduledFuture<?> handle = scheduler.scheduleAtFixedRate(new Runnable() {

            final Pattern pattern = Pattern.compile(matching);
            final ConcurrentSkipListMap<File, Tailer> files = new ConcurrentSkipListMap<>();
            final ExecutorService es = Executors.newCachedThreadPool();

            @Override
            public void run() {
                try {
                    disableInactiveTailers();

                    File[] logFiles = new File(directory).listFiles(pathname -> pattern.matcher(pathname.getName()).matches());

                    for (File logFile : logFiles) {
                        if (files.containsKey(logFile) &&
                                logFile.lastModified() < System.currentTimeMillis() - TimeUnit.HOURS.toMillis(1)) {
                            disableTailer(logFile, "Disabling listener for file due to inactivity: ");
                        } else if (logFile.lastModified() > (System.currentTimeMillis() - TimeUnit.HOURS.toMillis(1)) &&
                                !files.containsKey(logFile)) {
                            createTailer(logFile);
                        }
                    }
                } catch (Throwable e) {
                    logger.log(Level.WARNING, "Exception in listener creation", e);
                    // Keep running
                }
            }

            private void disableInactiveTailers() {
                for (File logFile : files.keySet()) {
                    if (logFile.exists()) {
                        continue;
                    }
                    disableTailer(logFile, "Disabling listener for the file since it no longer exists: ");
                }
            }

            private void disableTailer(File logFile, String msg) {
                logger.info(msg + logFile);
                Tailer tailer = files.remove(logFile);
                if (tailer != null) {
                    tailer.stop();
                }
            }

            private void createTailer(File logFile) {
                logger.info("Creating new listener for file: " + logFile);
                if (!logFile.exists()) {
                    logger.warning(logFile + " not found");
                    return;
                }

                if (!logFile.canRead()) {
                    logger.warning(logFile + " is not readable");
                    return;
                }

                Tailer tailer = new Tailer(logFile, new FDBLogListener(prefix, values, gauges, shardedReporter), 1000, true);
                es.submit(tailer);
                if (files.putIfAbsent(logFile, tailer) != null) {
                    // The put didn't succeed, stop the tailer.
                    tailer.stop();
                }
            }
        }, 0, FILE_PARSING_PERIOD, TimeUnit.SECONDS);
    }
}
