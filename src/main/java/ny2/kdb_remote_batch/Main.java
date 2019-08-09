package ny2.kdb_remote_batch;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import lombok.extern.slf4j.Slf4j;

/**
 * Args: $1 target kdb process handle (`:host:port:id:pw) $2- q script files
 */
@Slf4j
public class Main {

    // //////////////////////////////////////
    // Sttaic
    // //////////////////////////////////////

    public static void main(String[] args) throws Exception {
        log.info("Start.");
        
        // args
        Main instance = new Main();
        instance.parseArgs(args);
        log.info("kdb handle: {}", instance.kdbHandle);
        for (Path path : instance.filePathList) {
            log.info("q file: {}", path);
        }
        
        // Execute
        KdbRemoteBatchFacade kdbRemoteBatchFacade = new KdbRemoteBatchFacade(instance.kdbHandle);
        kdbRemoteBatchFacade.executeRemoteBatch(instance.filePathList);

        // End
        log.info("End.");
        System.exit(0);
    }

    static void usageExit(String[] args, CmdLineParser parser) {
        System.out.println("Invalid args. " + args);
        System.out.println("Options:");
        parser.printUsage(System.out);
        System.exit(0);
    }

    // //////////////////////////////////////
    // Filed
    // //////////////////////////////////////

    //
    // args option
    //

    @Option(name = "-c", aliases = "--connect", usage = "kdb handle - `:host:port(:id:pw)")
    private String kdbHandle;

    @Option(name = "-q", aliases = "--qscript", usage = "q script file")
    private String qFile;

    @Option(name = "-f", aliases = "--file", usage = "script list file")
    private String listFile;

    @Option(name = "-h", aliases = "--help", usage = "show usage message and exit")
    private boolean usageFlag;

    // file list
    private final List<Path> filePathList = new ArrayList<>();

    public void parseArgs(String[] args) throws IOException {
        CmdLineParser parser = new CmdLineParser(this);
        try {
            parser.parseArgument(args);
        } catch (CmdLineException e) {
            System.err.println(e.getMessage());
            parser.printUsage(System.err);
            System.exit(-1);
        }

        // help
        if (usageFlag) {
            usageExit(args, parser);
        }

        // kdb handle
        if (StringUtils.isBlank(kdbHandle)) {
            usageExit(args, parser);
        }

        // qscript
        if (StringUtils.isNotBlank(qFile)) {
            Path path = Paths.get(qFile);
            if (Files.notExists(path)) {
                log.error("File is not exist. path={}", qFile);
                usageExit(args, parser);
            }
            filePathList.add(path);
        }

        // list file
        if (StringUtils.isNotBlank(listFile)) {
            Path path = Paths.get(listFile);
            if (Files.notExists(path)) {
                log.error("File is not exist. path={}", listFile);
                usageExit(args, parser);
            }

            // read file
            List<String> lines = Files.readAllLines(path);
            for (String line : lines) {
                line = StringUtils.trimToEmpty(line);
                if (StringUtils.isBlank(line) || StringUtils.startsWith(line, "#")) {
                    continue;
                }
                Path filePath = Paths.get(line);
                if (Files.notExists(filePath)) {
                    log.error("File is not exist. path={}", line);
                    continue;
                }
                filePathList.add(filePath);
            }
        }

        // check
        if (filePathList.isEmpty()) {
            log.error("No q script file is set.");
            usageExit(args, parser);
        }
    }
}
