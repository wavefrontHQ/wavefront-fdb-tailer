package com.wavefront.integrations;

import com.codahale.metrics.*;
import com.codahale.metrics.graphite.Graphite;
import com.codahale.metrics.graphite.GraphiteReporter;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.AtomicDouble;
import com.wavefront.dropwizard.metrics.DropwizardMetricsReporter;
import com.wavefront.sdk.common.WavefrontSender;
import com.wavefront.sdk.common.clients.WavefrontClientFactory;
import org.apache.commons.io.input.Tailer;

import java.io.File;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;

/**
 * This class collects and periodically reports metrics via Wavefront Proxy, Wavefront Direct Ingestion, or GraphiteReporter.
 */
public class FDBMetricsReporter {

    private final static Logger logger = Logger.getLogger(FDBMetricsReporter.class.getCanonicalName());

    private final static int FILE_PARSING_PERIOD = 30;

    private final static int METRICS_REPORTING_PERIOD = 60;

    private final static int BATCH_SIZE = 50_000;

    private final static int MAX_QUEUE_SIZE = 100_000;

    private final static int FLUSH_INTERVAL_SECONDS = 60;

    static {
        SharedMetricRegistries.setDefault("defaultFDBMetrics", new MetricRegistry());
    }

    private ScheduledReporter reporter;

    private WavefrontSender wavefrontSender;

    private String directory;

    private String matching;

    private String prefix;

    private LoadingCache<String, AtomicDouble> values;

    private LoadingCache<String, Gauge<Double>> gauges;

    private String SERVICE_NAME = "fdbtailer";

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

        if (arguments.getServiceName() != null) {
            SERVICE_NAME = arguments.getServiceName();
        }

        if (arguments.getReporterType() == FDBMetricsReporterArguments.ReporterType.PROXY) {
            initProxy(arguments.getProxyHost(), arguments.getProxyPort());
        } else if (arguments.getReporterType() == FDBMetricsReporterArguments.ReporterType.DIRECT) {
            initDirect(arguments.getServer(), arguments.getToken(), arguments.getEndPoints());
        } else if (arguments.getReporterType() == FDBMetricsReporterArguments.ReporterType.GRAPHITE) {
            initGraphite(arguments.getGraphiteServer(), arguments.getGraphitePort());
        }
    }

    private void initDirect(String server, String token, List<Map<String, String>> endPoints) {
        WavefrontClientFactory wavefrontClientFactory = new WavefrontClientFactory();

        if (endPoints != null) {
            for (Map<String, String> endPointMap : endPoints) {
                for (Map.Entry<String, String> entry : endPointMap.entrySet()) {
                    String endPoint = "https://" + entry.getValue();
                    this.wavefrontSender = addWavefrontClient(wavefrontClientFactory, endPoint);
                }
            }
        } else {
            String endPoint = "https://" + token + "@" + server;
            this.wavefrontSender = addWavefrontClient(wavefrontClientFactory, endPoint);
        }

        this.reporter = DropwizardMetricsReporter.forRegistry(SharedMetricRegistries.getDefault()).
                withSource(getHostName()).
                withReporterPointTag("service", SERVICE_NAME).
                withJvmMetrics().
                build(this.wavefrontSender);
    }

    private void initProxy(String proxyHostname, int proxyPort) throws UnknownHostException {
        String proxyURL = "proxy://" + proxyHostname + ":" + proxyPort;
        WavefrontClientFactory wavefrontClientFactory = new WavefrontClientFactory();
        this.wavefrontSender = addWavefrontClient(wavefrontClientFactory, proxyURL);

        this.reporter = DropwizardMetricsReporter.forRegistry(SharedMetricRegistries.getDefault()).
                withSource(getHostName()).
                withReporterPointTag("service", SERVICE_NAME).
                withJvmMetrics().
                build(this.wavefrontSender);
    }

    private void initGraphite(String graphiteServer, int graphitePort) {
        final Graphite graphite = new Graphite(new InetSocketAddress(graphiteServer, graphitePort));

        this.reporter = GraphiteReporter.forRegistry(SharedMetricRegistries.getDefault()).
                convertRatesTo(TimeUnit.SECONDS).
                convertDurationsTo(TimeUnit.MILLISECONDS).
                filter(MetricFilter.ALL).
                build(graphite);
    }

    private WavefrontSender addWavefrontClient(WavefrontClientFactory wavefrontClientFactory, String client) {
            wavefrontClientFactory.addClient(client,
                    BATCH_SIZE,
                    MAX_QUEUE_SIZE,
                    FLUSH_INTERVAL_SECONDS,
                    Integer.MAX_VALUE);

            return wavefrontClientFactory.getClient();
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
        this.reporter.start(METRICS_REPORTING_PERIOD, TimeUnit.SECONDS);
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

                Tailer tailer = new Tailer(logFile, new FDBLogListener(prefix, values, gauges, wavefrontSender, SERVICE_NAME), 1000, true);
                es.submit(tailer);
                if (files.putIfAbsent(logFile, tailer) != null) {
                    // The put didn't succeed, stop the tailer.
                    tailer.stop();
                }
            }
        }, 0, FILE_PARSING_PERIOD, TimeUnit.SECONDS);
    }
}
