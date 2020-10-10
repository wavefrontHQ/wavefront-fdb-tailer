package com.wavefront.integrations;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.SharedMetricRegistries;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Charsets;
import com.google.common.base.Splitter;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.AtomicDouble;
import com.wavefront.sdk.common.WavefrontSender;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.input.Tailer;
import org.apache.commons.io.input.TailerListenerAdapter;
import org.w3c.dom.Document;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

import static java.lang.Character.*;


/**
 * This class reads FoundationDB logs and translates the content into metrics.
 */
public class FDBLogListener extends TailerListenerAdapter {

    private static final Logger logger = Logger.getLogger(FDBLogListener.class.getCanonicalName());

    private static final String END_TRACE = "</Trace>";
    private static final String CLUSTER_TAG_KEY = "ClusterFile=\"";

    private static DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();

    // Metrics
    private static Map<String, Counter> severityMetrics = new HashMap<>();

    private LoadingCache<String, AtomicDouble> values;

    private LoadingCache<String, Gauge<Double>> gauges;

    static {

    }

    private final Counter failed;

    private Tailer tailer;

    private String prefix;

    private Map<String, String> tags;

    private WavefrontSender wavefrontSender;

    private String addPrefix(String name) {
        return prefix + name;
    }

    private List<String> disabledMetrics;

    public FDBLogListener(String prefix, LoadingCache<String, AtomicDouble> values,
                          LoadingCache<String, Gauge<Double>> gauges, WavefrontSender wavefrontSender, String serviceName, List<String> disabledMetrics) {
        this.prefix = prefix;
        this.values = values;
        this.gauges = gauges;
        this.wavefrontSender = wavefrontSender;
        this.failed = SharedMetricRegistries.getDefault().counter(addPrefix("listener_failed"));
        this.tags = new HashMap<String, String>() {{put("service", serviceName);}};
        this.disabledMetrics = disabledMetrics;
    }

    @Override
    public void init(Tailer tailer) {
        this.tailer = tailer;
        // Check to see if this one is already complete
        RandomAccessFile raf = null;
        try {
            raf = new RandomAccessFile(tailer.getFile(), "r");
            raf.seek(raf.length() - END_TRACE.length());
            byte[] endTrace = new byte[END_TRACE.length()];
            raf.readFully(endTrace);
            String endOfFile = new String(endTrace, Charsets.US_ASCII);
            if (END_TRACE.equals(endOfFile)) {
                done();
            }

            // Code to get the cluster file for sharded reporter
            Scanner sc = new Scanner(tailer.getFile());
            sc.useDelimiter("\n");
            boolean found = false;
            while (sc.hasNextLine()) {
                String line = sc.nextLine();
                String clusterFile = getClusterFile(line);
                if (clusterFile != null) {
                    tags.put("cluster_file", clusterFile);
                    found = true;
                    break;
                }
            }
            assert(found);

        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            IOUtils.closeQuietly(raf);
        }
        initSeverityMetrics();
    }

    public String getClusterFile(String line) {

        int index = line.indexOf(CLUSTER_TAG_KEY);
        if (index == -1) {
            return null;
        }
        int startIndex = index + CLUSTER_TAG_KEY.length();
        int endIndex = line.indexOf('\"', startIndex);
        assert(endIndex != -1);
        return line.substring(startIndex, endIndex);
    }

    @Override
    public void fileNotFound() {
        done();
    }

    private void done() {
        if (tailer == null) {
            return;
        }

        logger.info("Stopping tailer for " + tailer.getFile());
        tailer.stop();
        this.tailer = null;
    }

    private void initSeverityMetrics() {
        for (String sev : Arrays.asList("10", "20", "30", "40", "50")) {
            severityMetrics.put(sev, SharedMetricRegistries.getDefault().counter(addPrefix("severity_" + sev)));
        }
    }

    @Override
    public void handle(Exception ex) {
        logger.log(Level.WARNING, "Exception in tailer", ex);
        failed.inc();
    }

    @Override
    public void handle(String line) {
        if (tailer == null) {
            return;
        }

        handleLine(line);
    }

    @VisibleForTesting
    void handleLine(String line) {
        if (line.equals(END_TRACE)) {
            // End of log file
            done();
        } else if (line.startsWith("<Event ")) {
            try {
                DocumentBuilder db = dbf.newDocumentBuilder();
                Document doc = db.parse(new ByteArrayInputStream(line.getBytes(Charsets.UTF_8)));
                NamedNodeMap map = doc.getDocumentElement().getAttributes();
                Node type = map.getNamedItem("Type");
                if (type != null && enableMetricReporting(type.getNodeValue())) {
                    switch (type.getNodeValue()) {
                        case "Role": {
                            // Track all transitions with booleans.  It isn't clear how often
                            // these are reported so we may not get a clear view all the time.
                            boolean begin = map.getNamedItem("Transition").getNodeValue().equals("Begin");
                            String port = getPort(map);
                            String as = map.getNamedItem("As").getNodeValue();
                            String metricName = addPrefix(port + ".role." + encode(as));
                            if (this.wavefrontSender == null) {
                                gauges.getUnchecked(metricName);
                            } else {
                                this.wavefrontSender.sendMetric(metricName,
                                        values.getUnchecked(metricName).doubleValue(),
                                        null,
                                        null,
                                        tags);
                            }
                            AtomicDouble value = values.getUnchecked(metricName);
                            value.set(begin ? 1 : 0);
                            break;
                        }
                        case "MachineMetrics": {
                            addDoubleGauges(map, "machine", Arrays.asList("CPUSeconds", "Mbps",
                                    "OutSegs", "RetransSegs"));
                            break;
                        }
                        case "ProcessMetrics": {
                            String port = getPort(map);
                            addDoubleGauges(map, port, Arrays.asList("CPU", "Mbps", "Disk",
                                    "File", "N2", "AIO", "Cache", "Main"));
                            addDoubleGauge(map, port, "Memory");
                            break;
                        }
                        case "StorageMetrics": {
                            String port = getPort(map);
                            addDoubleGauges(map, port, Arrays.asList("Fetch"));
                            addDoubleGauges(map, port, Arrays.asList("bytes", "Bytes", "StorageVersion",
                                    "DurableVersion", "LoopsPerSecond", "MutationBytesPerSecond",
                                    "QueriesPerSecond", "Query", "Version", "IdleTime",
                                    "ChangesPerSecond", "ElapsedTime", "BytesFetchedPerSecond"));
                            break;
                        }
                        case "MasterCommit": {
                            String port = getPort(map);
                            addDoubleGauges(map, "master." + port, Arrays.asList("CommittedTransactions",
                                    "SubmittedTransactions", "Mutations", "Commits"));
                            addDoubleGauges(map, "master" + port, Arrays.asList("Version", "CommittedVersion"));
                            break;
                        }
                        case "RkUpdate": {
                            addDoubleGauges(map, "ratekeeper." + getPort(map), Arrays.asList("StorageServers", "Proxies",
                                    "TLogs", "ReadReplyRate", "WorseFreeSpace", "TPSLimit", "ReleasedTPS"));
                            break;
                        }
                        case "TotalDataInFlight": {
                            addDoubleGauge(map, "inflight", "TotalBytes");
                            break;
                        }
                        case "MovingData": {
                            addDoubleGauge(map, "shards." + getPort(map), "AverageShardSize");
                            addDoubleGauges(map, "moving", Arrays.asList("InFlight", "InQueue",
                                    "LowPriorityRelocations", "HighPriorityRelocations", "HighestPriority"));
                            break;
                        }
                        case "MachineLoadDetail": {
                            addDoubleGauges(map, "load", Arrays.asList("User", "Nice", "System", "Idle", "IOWait", "Steal", "Guest"));
                            addDoubleGauges(map, "interrupts", Arrays.asList("IRQ", "SoftIRQ"));
                            break;
                        }
                        case "ProgramStart": {
                            addDoubleGauge(map, getPort(map) + ".start", "ActualTime");
                            break;
                        }
                        case "MemSample": {
                            // an entry look something like:
                            // <Event Severity="10" Time="1402522454.544848" Type="MemSample" Machine="0.0.0.0:0"
                            //        ID="0000000000000000" Count="133088" TotalSize="545128448" SampleCount="1"
                            //        Hash="FastAllocatedUnused4096" Bt="na"/>
                            String hash = map.getNamedItem("Hash").getNodeValue();
                            if (hash.startsWith("FastAllocatedUnused")) {
                                // remove the prefix.
                                hash = hash.substring("FastAllocatedUnused".length());
                                addDoubleGauges(map, "memsample.fast_allocated_unused." + hash, Arrays.asList("Count", "TotalSize"));
                            } else if (hash.equals("backTraces") || hash.equals("memSamples")) {
                                addDoubleGauges(map, "memsample." + encode(hash), Arrays.asList("Count", "TotalSize", "SampleCount"));
                            }
                            break;
                        }
                        case "MemSampleSummary": {
                            // an entry look something like:
                            // <Event Severity="10" Time="1402610771.246234" Type="MemSampleSummary" Machine="0.0.0.0:0"
                            //        ID="0000000000000000" InverseByteSampleRatio="10000000" MemorySamples="0" BackTraces="1"
                            //        TotalSize="0" TotalCount="0"/>
                            addDoubleGauges(map, "memsample", Arrays.asList("InverseByteSampleRatio", "MemorySamples", "BackTraces",
                                    "TotalSize", "TotalCount"));
                            break;
                        }
                        default: {
                            break;
                        }
                    }
                }
                Node severity = map.getNamedItem("Severity");
                if (severity != null) {
                    Counter counter = severityMetrics.get(severity.getNodeValue());
                    if (counter != null) {
                        counter.inc();
                    }
                }
            } catch (ParserConfigurationException | SAXException | IOException | IllegalArgumentException e) {
                logger.log(Level.SEVERE, "Failed to parse log line: " + line, e);
                throw new RuntimeException(e);
            }
        }
    }

    private void addDoubleGauges(NamedNodeMap map, String port, List<String> list) throws IOException {
        for (int i = 0; i < map.getLength(); ++i) {
            String name = map.item(i).getNodeName();
            for (String prefix : list) {
                if (name.startsWith(prefix)) {
                    addDoubleGauge(map, port, name);
                }
            }
        }
    }

    private void addDoubleGauge(NamedNodeMap map, String prefix, String name) throws IOException {
        String metricName = addPrefix(prefix + "." + encode(name));
        if (this.wavefrontSender == null) {
            gauges.getUnchecked(metricName);
        } else {
            this.wavefrontSender.sendMetric(metricName,
                    values.getUnchecked(metricName).doubleValue(),
                    null,
                    null,
                    tags);
        }
        AtomicDouble value = values.getUnchecked(metricName);
        Node nodeValue = map.getNamedItem(name);
        if (nodeValue != null) {
            extractDoubleFromNodeValue(value, nodeValue);
        }
    }

    private void extractDoubleFromNodeValue(AtomicDouble value, Node nodeValue) {
        try {
            value.set(Double.parseDouble(nodeValue.getNodeValue()));
        } catch (NumberFormatException ex) {
            // It is possible that the number has multiple parts and the last part is the actual value.
            value.set(Double.parseDouble(Iterables.getLast(Splitter.on(" ").split(nodeValue.getNodeValue()))));
        }
    }

    private String getPort(NamedNodeMap map) {
        Node machineNode = map.getNamedItem("Machine");
        if (machineNode == null) {
            throw new IllegalArgumentException("'Machine' attribute is missing");
        }
        String machine = machineNode.getNodeValue();
        return machine.substring(machine.indexOf(":") + 1);
    }

    private boolean enableMetricReporting(String nodeValue) {
        boolean enabled = true;
        if (disabledMetrics.toString().toLowerCase().contains(nodeValue.toLowerCase())) {
            enabled = false;
        }
        return enabled;
    }

    @VisibleForTesting
    String encode(String name) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < name.length(); ++i) {
            char ch = name.charAt(i);
            if (isUpperCase(ch)) {
                if ((i != 0 && i + 1 < name.length() && isLowerCase(name.charAt(i + 1))) ||
                        (i > 0 && isLowerCase(name.charAt(i - 1)))) {
                    sb.append("_");
                }
            }
            if (ch != '_') {
                sb.append(toLowerCase(ch));
            }
        }
        return sb.toString();
    }
}
