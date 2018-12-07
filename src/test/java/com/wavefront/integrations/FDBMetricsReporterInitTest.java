package com.wavefront.integrations;

import com.beust.jcommander.JCommander;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static com.wavefront.integrations.FDBMetricsReporterInit.isValid;
import static com.wavefront.integrations.FDBMetricsReporterInit.parseArguments;
import static org.junit.Assert.*;

/**
 * This class tests FDBMetricsReporterInit.
 *
 * @author Devon Warshaw (warshawd@vmware.com).
 */
public class FDBMetricsReporterInitTest {

    private FDBMetricsReporterArguments arguments;
    private FDBMetricsReporterInit init;
    private static final String FDB_LOG_DIR = "/usr/local/testLogs/logs";

    @Before
    public void setUp() {
        arguments = new FDBMetricsReporterArguments();
        init = new FDBMetricsReporterInit();
    }


    @Test
    public void testUnparsedParams() {
        String[] args = {"--dir", FDB_LOG_DIR, "unparsed1", "unparsed2"};
        JCommander jCommander = new JCommander(arguments, args);
        assertFalse(isValid(arguments));
        assertEquals(arguments.getDirectory(), FDB_LOG_DIR);
        List<String> unparsed = new ArrayList<>();
        unparsed.add("unparsed1");
        unparsed.add("unparsed2");
        assertEquals(arguments.getUnparsedParams(), unparsed);
    }

    @Test
    public void testConfigFile() {
        String proxyHost = "1.0.0.0";
        int proxyPort = 2000;
        String server = "wavefront.example.com";
        String token = "abcde";
        int graphitePort = 3000;
        String graphiteServer = "graphite.example.com";
        FDBMetricsReporterArguments.ReporterType type = FDBMetricsReporterArguments.ReporterType.PROXY;
        String[] args = {"-f", "src/test/resources/test_config.yaml"};
        try {
            parseArguments(args, init);
        } catch (Exception e) {
            e.printStackTrace();
            fail("Exception thrown: " + e.getMessage());
        }
        assertEquals(init.arguments.getProxyHost(), proxyHost);
        assertEquals(init.arguments.getProxyPort(), proxyPort);
        assertEquals(init.arguments.getToken(), token);
        assertEquals(init.arguments.getServer(), server);
        assertEquals(init.arguments.getGraphitePort(), graphitePort);
        assertEquals(init.arguments.getGraphiteServer(), graphiteServer);
        assertEquals(init.arguments.getReporterType(), type);
    }

    @Test
    public void commandLineOverrideTest() {
        String proxyHost = "1.0.0.0";
        int proxyPort = 2000;
        String server = "localhost";
        String token = "abcdefg";
        int graphitePort = 3000;
        String graphiteServer = "graphite.example.com";
        String dir = "/test/dir";
        String matching = "$a";
        FDBMetricsReporterArguments.ReporterType type = FDBMetricsReporterArguments.ReporterType.GRAPHITE;
        String[] args = {"--type", "GRAPHITE", "--dir", "/test/dir", "--matching", "$a", "--server", "localhost", "--token", "abcdefg", "-f", "src/test/resources/test_config.yaml"};
        try {
            parseArguments(args, init);

        } catch (Exception e) {
            e.printStackTrace();
            e.getMessage();
            fail("Exception thrown");
        }
        assertEquals(init.arguments.getProxyHost(), proxyHost);
        assertEquals(init.arguments.getProxyPort(), proxyPort);
        assertEquals(init.arguments.getToken(), token);
        assertEquals(init.arguments.getServer(), server);
        assertEquals(init.arguments.getGraphitePort(), graphitePort);
        assertEquals(init.arguments.getGraphiteServer(), graphiteServer);
        assertEquals(init.arguments.getReporterType(), type);
        assertEquals(init.arguments.getDirectory(), dir);
        assertEquals(init.arguments.getMatching(), matching);

    }

}