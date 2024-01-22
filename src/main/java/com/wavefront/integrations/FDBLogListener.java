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
                            // an entry looks something like:
                            // <Event Severity="10" Time="1622081571.719325" Type="MovingData" ID="8fb39ddb70631e31"
                            // InFlight="845" InQueue="315666" AverageShardSize="250000000" UnhealthyRelocations="143"
                            // HighestPriority="950" BytesWritten="346668575426" PriorityRecoverMove="0" PriorityRebalanceUnderutilizedTeam="40"
                            // PriorityRebalanceOverutilizedTeam="36" PriorityTeamHealthy="0" PriorityTeamContainsUndesiredServer="0"
                            // PriorityTeamRedundant="4" PriorityMergeShard="264698" PriorityPopulateRegion="0" PriorityTeamUnhealthy="0"
                            // PriorityTeam2Left="0" PriorityTeam1Left="0" PriorityTeam0Left="0" PrioritySplitShard="51733"
                            // Machine="10.5.1.101:4591" LogGroup="default" Roles="DD" TrackLatestType="Original" />
                            addDoubleGauge(map, "shards." + getPort(map), "AverageShardSize");
                            addDoubleGauges(map, "moving", Arrays.asList("InFlight", "InQueue",
                                    "LowPriorityRelocations", "HighPriorityRelocations", "HighestPriority",
                                    "UnhealthyRelocations", "BytesWritten", "Priority"));
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
                        case "RedwoodMetrics": {
                            // an entry looks something like:
                            // BTreePreload="919" BTreePreloadExt="60" OpSet="10377" OpSetKeyBytes="375634" OpSetValueBytes="26560987" OpClear="806" OpClearKey="0" OpGet="75" OpGetRange="1403" OpCommit="10"
                            // PagerDiskWrite="4298" PagerDiskRead="1324" PagerCacheHit="8848" PagerCacheMiss="1183" PagerProbeHit="1" PagerProbeMiss="23" PagerEvictUnhit="3222" PagerEvictFail="0"
                            // PagerRemapFree="207" PagerRemapCopy="24" PagerRemapSkip="176" LookupGetRPF="919" LookupMeta="53" HitGetRPF="61" HitMeta="20" MissGetRPF="858" MissMeta="9"
                            // WriteMeta="74" PageCacheCount="189076" PageCacheMoved="0" PageCacheSize="2147481730" DecodeCacheSize="46733442" L1PageBuild="2449" L1PageBuildExt="1578"
                            // L1PageModify="105" L1PageModifyExt="5" L1PageRead="2942" L1PageReadExt="729" L1PageCommitStart="403" L1LazyClearInt="0" L1LazyClearIntExt="0" L1LazyClear="0"
                            // L1LazyClearExt="0" L1ForceUpdate="0" L1DetachChild="0" L1LookupCommit="403" L1LookupLazyClr="0" L1LookupGet="75" L1LookupGetR="2464" L1HitCommit="389" L1HitLazyClr="0"
                            // L1HitGet="61" L1HitGetR="2222" L1MissCommit="14" L1MissLazyClr="0" L1MissGet="14" L1MissGetR="242" L1WriteCommit="4137" L1WriteLazyClr="0" L2PageBuild="20" L2PageBuildExt="0"
                            // L2PageModify="49" L2PageModifyExt="0" L2PageRead="1583" L2PageReadExt="0" L2PageCommitStart="101" L2LazyClearInt="0" L2LazyClearIntExt="0" L2LazyClear="0" L2LazyClearExt="0"
                            // L2ForceUpdate="13" L2DetachChild="68" L2LookupCommit="101" L2LookupLazyClr="0" L2LookupGet="75" L2LookupGetR="1407" L2HitCommit="96" L2HitLazyClr="0" L2HitGet="72" L2HitGetR="1377"
                            // L2MissCommit="5" L2MissLazyClr="0" L2MissGet="3" L2MissGetR="30" L2WriteCommit="69" L2WriteLazyClr="0" L3PageBuild="0" L3PageBuildExt="0" L3PageModify="16" L3PageModifyExt="0"
                            // L3PageRead="1551" L3PageReadExt="0" L3PageCommitStart="73" L3LazyClearInt="0" L3LazyClearIntExt="0" L3LazyClear="0" L3LazyClearExt="0" L3ForceUpdate="6" L3DetachChild="19"
                            // L3LookupCommit="73" L3LookupLazyClr="0" L3LookupGet="75" L3LookupGetR="1403" L3HitCommit="73" L3HitLazyClr="0" L3HitGet="75" L3HitGetR="1395" L3MissCommit="0" L3MissLazyClr="0" L3MissGet="0"
                            // L3MissGetR="8" L3WriteCommit="16" L3WriteLazyClr="0" L4PageBuild="0" L4PageBuildExt="0" L4PageModify="2" L4PageModifyExt="0" L4PageRead="1519" L4PageReadExt="0" L4PageCommitStart="41"
                            // L4LazyClearInt="0" L4LazyClearIntExt="0" L4LazyClear="0" L4LazyClearExt="0" L4ForceUpdate="2" L4DetachChild="7" L4LookupCommit="41" L4LookupLazyClr="0" L4LookupGet="75" L4LookupGetR="1403"
                            // L4HitCommit="41" L4HitLazyClr="0" L4HitGet="75" L4HitGetR="1403" L4MissCommit="0" L4MissLazyClr="0" L4MissGet="0" L4MissGetR="0" L4WriteCommit="2" L4WriteLazyClr="0" L5PageBuild="0"
                            // L5PageBuildExt="0" L5PageModify="0" L5PageModifyExt="0" L5PageRead="1488" L5PageReadExt="0" L5PageCommitStart="10" L5LazyClearInt="0" L5LazyClearIntExt="0" L5LazyClear="0" L5LazyClearExt="0"
                            // L5ForceUpdate="0" L5DetachChild="0" L5LookupCommit="10" L5LookupLazyClr="0" L5LookupGet="75" L5LookupGetR="1403" L5HitCommit="10" L5HitLazyClr="0" L5HitGet="75" L5HitGetR="1403"
                            // L5MissCommit="0" L5MissLazyClr="0" L5MissGet="0" L5MissGetR="0" L5WriteCommit="0" L5WriteLazyClr="0" ThreadID="13425918326275095525" Machine="10.0.0.1:4502" LogGroup="default" Roles="SS" />
                            String port = getPort(map);
                            addDoubleGauges(map, port, Arrays.asList(
                                    "BTreePreload", "BTreePreloadExt", "OpSetKeyBytes", "OpSetValueBytes", "OpClear", "OpClearKey", "OpGet", "OpGetRange", "OpCommit",
                                    "PagerDiskWrite", "PagerDiskRead", "PagerCacheHit=", "PagerCacheMiss", "PagerProbeHit", "PagerProbeMiss", "PagerEvictUnhit", "PagerEvictFail",
                                    "PagerRemapFree", "PagerRemapCopy", "PagerRemapSkip", "LookupGetRPF", "LookupMeta", " HitGetRPF", "HitMeta", "MissGetRPF", "MissMeta",
                                    "WriteMeta", "PageCacheCount", "PageCacheMoved", "PageCacheSize", "DecodeCacheSize", "L1PageBuild", "L1PageBuildExt",
                                    "L1PageModify", "L1PageModifyExt", "L1PageRead", "L1PageReadExt", "L1PageCommitStart", "L1LazyClearInt", "L1LazyClearIntExt", "L1LazyClear",
                                    "L1LazyClearExt", "L1ForceUpdate", "L1DetachChild", "L1LookupCommit", "L1LookupLazyClr", "L1LookupGet", "L1LookupGetR", "L1HitCommit", "L1HitLazyClr",
                                    "L1HitGet", "L1HitGetR", "L1MissCommit", "L1MissLazyClr", "L1MissGet", "L1MissGetR", "L1WriteCommit", "L1WriteLazyClr", "L2PageBuild", "L2PageBuildExt",
                                    "L2PageModify", "L2PageModifyExt", "L2PageRead", "L2PageReadExt", "L2PageCommitStart", "L2LazyClearInt", "L2LazyClearIntExt", "L2LazyClear", "L2LazyClearExt",
                                    "L2ForceUpdate", "L2DetachChild", "L2LookupCommit", "L2LookupLazyClr", "L2LookupGet", "L2LookupGetR", "L2HitCommit", "L2HitLazyClr", "L2HitGet", "L2HitGetR",
                                    "L2MissCommit", "L2MissLazyClr", "L2MissGet", "L2MissGetR", "L2WriteCommit", "L2WriteLazyClr", "L3PageBuild", "L3PageBuildExt", "L3PageModify", "L3PageModifyExt",
                                    "L3PageRead", "L3PageReadExt", "L3PageCommitStart", "L3LazyClearInt", "L3LazyClearIntExt", "L3LazyClear", "L3LazyClearExt", "L3ForceUpdate", "L3DetachChild",
                                    "L3LookupCommit", "L3LookupLazyClr", "L3LookupGet", "L3LookupGetR", "L3HitCommit", "L3HitLazyClr", "L3HitGet", "L3HitGetR", "L3MissCommit", "L3MissLazyClr", "L3MissGet",
                                    "L3MissGetR", "L3WriteCommit", "L3WriteLazyClr", "L4PageBuild", "L4PageBuildExt", "L4PageModify", "L4PageModifyExt", "L4PageRead", "L4PageReadExt", "L4PageCommitStart",
                                    "L4LazyClearInt", "L4LazyClearIntExt", "L4LazyClear", "L4LazyClearExt", "L4ForceUpdate", "L4DetachChild", "L4LookupCommit", "L4LookupLazyClr", "L4LookupGet" ,"L4LookupGetR",
                                    "L4HitCommit", "L4HitLazyClr", "L4HitGet", "L4HitGetR", "L4MissCommit", "L4MissLazyClr", "L4MissGet", "L4MissGetR", "L4WriteCommit", "L4WriteLazyClr", "L5PageBuild",
                                    "L5PageBuildExt", "L5PageModify", "L5PageModifyExt", "L5PageRead", "L5PageReadExt", "L5PageCommitStart", "L5LazyClearInt", "L5LazyClearIntExt", "L5LazyClear", "L5LazyClearExt",
                                    "L5ForceUpdate", "L5DetachChild", "L5LookupCommit", "L5LookupLazyClr", "L5LookupGet", "L5LookupGetR", "L5HitCommit", "L5HitLazyClr", "L5HitGet", "L5HitGetR",
                                    "L5MissCommit", "L5MissLazyClr", "L5MissGet", "L5MissGetR", "L5WriteCommit", "L5WriteLazyClr"
                            ));
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
