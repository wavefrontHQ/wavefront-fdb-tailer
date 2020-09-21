# FoundationDB Metrics

The fdb-metrics Java application parses FoundationDB logs into metrics and sends them to Wavefront using a [Wavefront Proxy](https://docs.wavefront.com/proxies.html) or [Wavefront direct ingestion](https://docs.wavefront.com/direct_ingestion.html).  It can also be configured to send metrics to a Graphite server by using a [Graphite Reporter](https://metrics.dropwizard.io/3.1.0/manual/graphite/).

#### What is Wavefront?
[Wavefront](https://wavefront.com) is a SaaS-based metrics and observability platform.  More info can be found on the website linked, where you can also schedule a [30-day free trial](https://www.wavefront.com/sign-up/) or a [one-on-one demo](https://www.wavefront.com/schedule-a-meeting/)

## Installation
This application is built from a maven repository. Simply run ```mvn clean install``` from the cloned repo and run the resulting jar.


## Usage

### Configuration

The fdb-metrics application can be configured via a YAML configuration file specified at the command line, or it can be configured entirely via command line arguments.

### Command Line Configuration
The command line argument options are as follows:

```
--dir, -d
     Absolute path to search for FDB logs.
     
--file, -f
     Path to the yaml config file that specifies the parameters, if
     not passed in via command line.
     
--graphitePort
     The port for the Graphite Server.  Used only if --type is set to
     GRAPHITE.

--graphiteServer
     The name of the machine running a Graphite server.  Used only if --type
     is set to GRAPHITE.
     
--help, -h
     Prints available options.
     
--matching, -m
     A regex expression to match against potential log files in the directory.
     Default: .*
     
--prefix
     A prefix to attach to all metrics collected.  The default is "fdb.trace." if not specified.
     
--proxyHost
     The name of the machine running a Wavefront proxy.  Used only if --type
     is set to PROXY.
     Default: localhost
     
--proxyPort
     The port the Wavefront proxy is listening on.  Used only if --type is set
     to PROXY.
     Default: 2878
     
--server
     The name of the machine for Wavefront direct ingestion.  Used only if
     --type is set to DIRECT.
     
--token
     The API token for Wavefront direct ingestion. Used only if --type is set to
     DIRECT.

--endPoints
    A list of Wavefront endpoints to send metrics. Used only if --type is set to DIRECT.

--serviceName
    Optional variable to control the name of the service reported. Defaults to fdbtailer.
     
--type
     The type of reporter that should be used to report the metrics gathered.
     Possible Values: [DIRECT, PROXY, GRAPHITE]

--disabledMetrics
    Option to disable certain metrics collected by FDBTailer.
    Possible Values: [role, machineMetrics, processMetrics, storageMetrics, masterCommit, rkUpdate, totalDataInFlight, movingData, machineLoadDetail, programStart, memSample, memSampleSummary]

```

### Configuration via YAML
You can configure the application by specifying a YAML configuration file at the command line, like this:
```
    --file <file_path>
```

All of the potentional YAML configuration options correspond to command line options, and are listed here:

```
directory:
graphitePort:
graphiteServer:
matching:
prefix:
proxyHost:
proxyPort:
reporterType:
server:
token:
endPoints:
serviceName
```

An example YAML configuration file is included as example_config.yaml in this repo, and is reproduced here:

```
directory: "/usr/local/foundationdb/logs"
matching: ".*\\.xml$"
reporterType: PROXY
proxyHost: 127.0.0.1
proxyPort: 2878
disabledMetrics:
  - "machineLoadDetail"
  - "memSampleSummary"
```

This is a simple configuration that sets up the fdb-metrics application to send metrics to a Wavefront proxy running on the local machine.  It will examine all files ending with ```.xml``` in the directory ```/usr/local/foundationdb/logs```.

## Using Different Reporters
Currently the application supports three different metric reporters: the Wavefront Proxy reporter, the Wavefront direct ingestion reporter, and the Graphite reporter.  Three things need to be specified regardless of which reporter is being used:
  * The ```directory```, the absolute path to search for logs
  * The ```matching``` regex pattern to match files in that directory against
  * The ```reporterType``` to specify which reporter should be used.

In addition, you have to specify two reporter-specific arguments for each of the different reporters, which are described in the next sections.
### Using the Wavefront Proxy Reporter

The Wavefront Proxy reporter requires a [Wavefront Proxy](https://docs.wavefront.com/proxies.html) to be running and accessible.  You will need to provide the proxy address as ```proxyHost``` and the port the proxy is listening on as ```proxyPort```. If not provided, the default values will send to 127.0.0.1 at port 2878. 
Here is an example of the command line options that configure the application to send metrics to a Wavefront Proxy running on the local machine.

```
    --proxyHost 127.0.0.1
    --dir "/usr/local/foundationdb/logs"
    --matching ".*\\.xml$"
    --type PROXY
```

### Using the Direct Ingestion Reporter

In order to use the direct ingestion reporter, you will need to provide the ```server``` address and the [Wavefront API](https://docs.wavefront.com/wavefront_api.html#generating-an-api-token) ```token```
Here is an example of the options to provide in a YAML configuration file for direct ingestion, when the server is running on the local machine.

```
    server: "http://localhost:8080"
    token: <wavefront_api_token>
    directory: "/usr/local/foundationdb/logs"
    matching: ".*\\.xml$"
    reporterType: DIRECT
```

### Using the Graphite Reporter

If you are running a Graphite server, a Graphite reporter can also be used by specifying the ```graphiteServer``` and ```graphitePort``` corresponding to server.  For example, a Graphite server running on graphite.example.com and listening to port 2003 could be specified on the command line via:

```
    --type GRAPHITE
    --graphiteServer graphite.example.com
    --graphitePort 2003
    --dir "/usr/local/foundationdb/logs"
    --matching ".*"
```

### Using multiple Wavefront endpoints

In order to send metrics to multiple Wavefront endpoint, you will need to provide a list of endpoints and the [Wavefront API](https://docs.wavefront.com/wavefront_api.html#generating-an-api-token) token. Here is an example of the options to provide in a YAML configuration file. Note that the reporter type must be `DIRECT`. 
```
    directory: "/usr/local/foundationdb/logs"
    matching: ".*\\.xml$"
    reporterType: DIRECT
    endPoints:
        - endPoint1: token@endPoint1.wavefront.com
        - endPoint2: token@endPoint2.wavefront.com
```
