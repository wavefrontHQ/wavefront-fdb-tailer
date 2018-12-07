package com.wavefront.integrations;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import static org.junit.Assert.*;

/**
 * This class tests the parsing of a fdb log file.
 *
 * @author Clement Pang (clement@wavefront.com).
 */
public class FDBLogListenerTest {

    private FDBLogListener listener;

    static {
        SharedMetricRegistries.setDefault("defaultFDBMetrics", new MetricRegistry());
    }

    @Before
    public void setUp() {
        listener = new FDBLogListener();
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
    public void testEcoder() {
        assertEquals("cpu_seconds", listener.encode("CPUSeconds"));
        assertEquals("mbps_sent", listener.encode("MbpsSent"));
        assertEquals("n2_yield_calls", listener.encode("N2_YieldCalls"));
        assertEquals("aio_submit_lag_ms", listener.encode("AIO_SubmitLagMS"));
    }
}