package com.wavefront.integrations;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.AtomicDouble;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;

/**
 * This class tests the parsing of a fdb log file.
 *
 * @author Clement Pang (clement@wavefront.com).
 */
public class FDBLogListenerTest {

    private FDBLogListener listener;

    private String prefix = "fdb.trace.";

    private String sampleLogLine = "<Event Severity=\"10\" Time=\"1555440637.134462\" OriginalTime=\"1555357055.000500\" Type=\"ClientStart\" Machine=\"10.9.0.61:5911\" ID=\"0000000000000000\" SourceVersion=\"e6d43ba4c52200b534529820e33cddeae402ab3a\" Version=\"5.2.26-PRERELEASE\" PackageName=\"5.2\" ClusterFile=\"/etc/foundationdb/fdb.cluster\" ConnectionString=\"telemetry:asdfasdf@10.9.0.15:4500,10.9.0.29:4500,10.9.0.37:4500\" ActualTime=\"1555357054\" ApiVersion=\"-1\" ImageOffset=\"(nil)\" logGroup=\"default\" TrackLatestType=\"Rolled\"/>";

    private String metricName(String name) {
        return prefix + name;
    }

    private String serviceName = "fdbtailer";

    private List<String> disabledMetrics = Arrays.asList("machineMetrics");

    static {
        SharedMetricRegistries.setDefault("defaultFDBMetrics", new MetricRegistry());
    }

    @Before
    public void setUp() {

        LoadingCache<String, AtomicDouble> values = CacheBuilder.newBuilder().build(
                new CacheLoader<String, AtomicDouble>() {
                    @Override
                    public AtomicDouble load(String key) {
                        return new AtomicDouble();
                    }
                });

        LoadingCache<String, Gauge<Double>> gauges = CacheBuilder.newBuilder().build(
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

        listener = new FDBLogListener(prefix, values, gauges, null, serviceName, disabledMetrics);
    }

    @Test
    public void testParsing() throws IOException {
        BufferedReader br = new BufferedReader(new InputStreamReader(getClass().getResourceAsStream("fdb.log")));
        String line;
        while ((line = br.readLine()) != null) {
            listener.handleLine(line);
        }
        assertEquals(SharedMetricRegistries.getDefault().getMetrics().size(), 25);
    }

    @Test
    public void testShardParsing() {
        String clusterFile = listener.getClusterFile(sampleLogLine);
        assert(clusterFile != null);
        System.out.println(clusterFile);
    }

    @Test
    public void testEcoder() {
        assertEquals("cpu_seconds", listener.encode("CPUSeconds"));
        assertEquals("mbps_sent", listener.encode("MbpsSent"));
        assertEquals("n2_yield_calls", listener.encode("N2_YieldCalls"));
        assertEquals("aio_submit_lag_ms", listener.encode("AIO_SubmitLagMS"));
    }
}
