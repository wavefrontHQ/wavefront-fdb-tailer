package com.wavefront.integrations;

import com.beust.jcommander.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import org.apache.commons.lang3.ObjectUtils;

import java.io.File;
import java.net.UnknownHostException;
import java.util.concurrent.Semaphore;
import java.util.logging.Level;
import java.util.logging.Logger;

public class FDBMetricsReporterInit {

    FDBMetricsReporterInit() {
        this.arguments = new FDBMetricsReporterArguments();
    }

    private static final Logger logger = Logger.getLogger(FDBMetricsReporterArguments.class.getCanonicalName());

    public static class FileConverter implements IStringConverter<File> {
        @Override
        public File convert(String value) {
            return new File(value);
        }
    }

    public FDBMetricsReporterArguments arguments;


    private boolean isHelp() {
        return arguments.isHelp();
    }

    private void printUnparsedParams() {
        if (arguments.getUnparsedParams() != null) {
            logger.warning("Unparsed arguments: " + Joiner.on(", ").join(arguments.getUnparsedParams()));
        }
    }

    @VisibleForTesting
    static boolean isValid(FDBMetricsReporterArguments arguments) {
        if (arguments.getReporterType() == FDBMetricsReporterArguments.ReporterType.GRAPHITE) {
            if (arguments.getGraphitePort() == 0 || arguments.getGraphiteServer() == null) {
                return false;
            }
        } else if (arguments.getReporterType() == FDBMetricsReporterArguments.ReporterType.DIRECT) {
            if (arguments.getServer() == null || arguments.getToken() == null) {
                return false;
            }
        } else if (arguments.getReporterType() == FDBMetricsReporterArguments.ReporterType.PROXY ||
                    arguments.getReporterType() == FDBMetricsReporterArguments.ReporterType.SHARDED) {
            if (arguments.getProxyHost() == null) {
                return false;
            }
        } else {
            return false;
        }
        return true;
    }

    @VisibleForTesting
    static void readConfig(FDBMetricsReporterInit init) {
        if (init.arguments.getConfigFile() != null) {
            ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
            try {
                init.arguments = mapper.readValue(init.arguments.getConfigFile(), FDBMetricsReporterArguments.class);
            } catch (Exception e) {
                logger.log(Level.SEVERE, "Failed to parse yaml config file: " + init.arguments.getConfigFile(), e);
                System.exit(-1);
            }
        }
    }

    @VisibleForTesting
    static void parseArguments(String[] args, FDBMetricsReporterInit init) {
        JCommander jCommander = new JCommander(init.arguments, args);
        if (init.isHelp()) {
            jCommander.setProgramName(FDBMetricsReporter.class.getCanonicalName());
            jCommander.usage();
            System.exit(0);
        }
        init.printUnparsedParams();

        readConfig(init);

        // Override the config file with whatever command line arguments were specified.
        JCommander CLIOverride = new JCommander(init.arguments, args);
    }

    public static void main(String[] args) throws UnknownHostException {
        logger.info("Arguments: " + Joiner.on(", ").join(args));
        FDBMetricsReporterInit init = new FDBMetricsReporterInit();

        parseArguments(args, init);

        if (!isValid(init.arguments)) {
            logger.log(Level.SEVERE, "Not enough options specified for the reporter type specified.");
            System.exit(-1);
        }

        FDBMetricsReporter reporter = new FDBMetricsReporter(init.arguments);
        reporter.start();

        // Block indefinitely while the reporter continues to run.
        Semaphore semaphore = new Semaphore(0);
        semaphore.acquireUninterruptibly();
    }
}
