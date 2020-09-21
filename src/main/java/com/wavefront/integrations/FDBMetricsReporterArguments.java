package com.wavefront.integrations;

import com.beust.jcommander.Parameter;

import java.io.File;
import java.util.List;
import java.util.Map;

/**
 * This class holds the configuration parameters for the fdb-metrics jar.
 */
public class FDBMetricsReporterArguments {

    enum ReporterType {
        DIRECT, PROXY, GRAPHITE;
    }

    private static final String ALL_FILES = ".*";

    private static final String DEFAULT_HOST = "localhost";

    private static final int DEFAULT_PORT = 2878;

    private static final String DEFAULT_SERVICE_NAME = "fdbtailer";

    /**
     * @param reporterType The type of reporter that should be used to report the metrics gathered.  Current options are PROXY,
     *             DIRECT, GRAPHITE, and SHARDED.
     */
    @Parameter(names = {"--type"}, description = "The type of reporter that should be used to report the metrics gathered.")
    private ReporterType reporterType;

    /**
     * @param directory The absolute path where loghead will search for FDB logs.
     */
    @Parameter(names = {"--dir", "-d"}, description = "The absolute path where loghead will search for FDB logs.")
    private String directory;

    /**
     * @param matching A regex expression to match against potential log files in the directory.  The default is .*, all files.
     */
    @Parameter(names = {"--matching", "-m"},
               description = "A regex expression to match against potential log files in the directory.")
    private String matching = ALL_FILES;

    /**
     * @param proxyHost The name of the machine running a Wavefront proxy.  Only used if --type is set to PROXY.
     */
    @Parameter(names = {"--proxyHost"},
               description = "The name of the machine running a Wavefront proxy.  Only used if --type is set to PROXY.")
    private String proxyHost = DEFAULT_HOST;

    /**
     * @param proxyPort The port the Wavefront proxy is listening on.  Only used if --type is set to PROXY.
     */
    @Parameter(names = {"--proxyPort"},
               description = "The port the Wavefront proxy is listening on.  Only used if --type is set to PROXY.")
    private int proxyPort = DEFAULT_PORT;

    /**
     * @param server The name of the machine for Wavefront direct ingestion.  Only used if --type is set to DIRECT.
     */
    @Parameter(names = {"--server"},
               description = "The name of the machine for Wavefront direct ingestion.  Only used if --type is set to DIRECT.")
    private String server;

    /**
     * @param token The token for Wavefront direct ingestion.  Only used if reporterType is set to DIRECT.
     */
    @Parameter(names = {"--token"},
               description = "The token for Wavefront direct ingestion.  Only used if --type is set to DIRECT.")
    private String token;

    /**
     * @param endPoints Array of URL endpoints to send metrics to. Only used if reporterType is set to DIRECT.
     */
    @Parameter(names = {"--endPoints"},
            description = "List of endpoints if sending to multiple Wavefront instances. Accepts a comma separated" +
                    "list in the form of `token@wavefronturl`. Only used if --type is set to DIRECT.")
    private List<Map<String, String>> endPoints;

    /**
     * @param graphiteServer The name of the machine running a Graphite server.  Only used if reporterType is set to
     *                       GRAPHITE.
     */
    @Parameter(names = {"--graphiteServer"},
            description = "The name of the machine running a Graphite server.  Only used if --type is set to GRAPHITE.")
    private String graphiteServer;

    /**
     * @param graphitePort The port the Graphite server is listening on.  Only used if reporterType is set to GRAPHITE.
     */
    @Parameter(names = {"--graphitePort"},
               description = "The port for the Graphite Server.  Only used if --type is set to GRAPHITE.")
    private int graphitePort;

    /**
     * @param help Whether usage information should be displayed.
     */
    @Parameter(names = {"--help", "-h"}, description = "Prints available options", help = true)
    private boolean help;

    /**
     * @param configFile The yaml config file that specifies the parameters, if not passed in via the command line.
     */
    @Parameter(names = {"--file", "-f"}, converter = FDBMetricsReporterInit.FileConverter.class,
               description = "The path to the yaml config file that specifies the parameters, if not passed in via command line.")
    private File configFile;

    /**
     * @param prefix An optional string used as a prefix for all metric Names.  Defaults to \"fdb.trace.\ if not specified."
     */
    @Parameter(names = {"--prefix"},
            description = "A prefix to attach to all metrics collected.  The default is \"fdb.trace.\" if not specified.")
    private String prefix = "fdb.trace.";

    /**
     * @param serviceName An optional name for the service. Defaults to \"fdbtailer\" if not specified.
     */
    @Parameter(names = {"--serviceName", "-n"},
            description = "Change the service name to another string. The default is \"fdbtailer\" if not specified.")
    private String serviceName = DEFAULT_SERVICE_NAME;

    /**
     * @param disableMetrics Disables metric types provided. Possible values: role, machineMetrics, processMetrics,
     *                       storageMetrics, masterCommit, rkUpdate, totalDataInFlight, movingData, machineLoadDetails,
     *                       programStart, memSample, memSampleSummary
     */
    private List<String> disabledMetrics;


    @Parameter(description = "")
    private List<String> unparsedParams;

    public void setDirectory(String directory) {
        this.directory = directory;
    }

    public void setMatching(String matching) {
        this.matching = matching;
    }

    public void setProxyHost(String proxyHost) {
        this.proxyHost = proxyHost;
    }

    public void setProxyPort(int proxyPort) {
        this.proxyPort = proxyPort;
    }

    public void setServer(String server) {
        this.server = server;
    }

    public void setToken(String token) {
        this.token = token;
    }

    public void setEndPoints(List<Map<String, String>> endPoints) {
        this.endPoints = endPoints;
    }

    public void setGraphiteServer(String graphiteServer) {
        this.graphiteServer = graphiteServer;
    }

    public void setGraphitePort(int graphitePort) {
        this.graphitePort = graphitePort;
    }

    public void setReporterType(ReporterType reporterType) {
        this.reporterType = reporterType;
    }

    public void setServiceName(String serviceName) {
        this.serviceName = serviceName;
    }

    public void setDisabledMetrics(List<String> disabledMetrics) { this.disabledMetrics = disabledMetrics; }

    public String getDirectory() {
        return directory;
    }

    public String getMatching() {
        return matching;
    }

    public String getProxyHost() {
        return proxyHost;
    }

    public int getProxyPort() {
        return proxyPort;
    }

    public String getServer() {
        return server;
    }

    public String getToken() {
        return token;
    }

    public List<Map<String, String>> getEndPoints() { return endPoints; }

    public String getGraphiteServer() {
        return graphiteServer;
    }

    public int getGraphitePort() {
        return graphitePort;
    }

    public ReporterType getReporterType() {
        return reporterType;
    }

    public boolean isHelp() {
        return help;
    }

    public void setHelp(boolean help) {
        this.help = help;
    }

    public File getConfigFile() {
        return configFile;
    }

    public void setConfigFile(File configFile) {
        this.configFile = configFile;
    }

    public String getPrefix() {
        return prefix;
    }

    public void setPrefix(String prefix) {
        this.prefix = prefix;
    }

    public List<String> getUnparsedParams() {
        return unparsedParams;
    }

    public String getServiceName() { return serviceName; }

    public List<String> getDisabledMetrics() { return disabledMetrics; }
}
