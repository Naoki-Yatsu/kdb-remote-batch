package ny2.kdb_remote_batch;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {
    
    private static final Logger logger = LoggerFactory.getLogger(Main.class);
    
    /**
     * 
     * Args: $1 target kdb process handle (`:host:port:id:pw)
     *       $2- q script files
     * 
     * @param args
     */
    public static void main(String[] args) {
        if (args.length < 2) {
            usageExit(args);
        }
        // $1 - handle
        String kdbHandle = StringUtils.trimToEmpty(args[0]);
        if (StringUtils.isBlank(kdbHandle)) {
            usageExit(args);
        }
        // $2 - file
        List<Path> filaPathList = new ArrayList<>();
        for (int i = 1; i < args.length; i++) {
            Path path = Paths.get(args[1]);
            if (Files.notExists(path)) {
                logger.error("File is not exist. path={}", path);
                continue;
            }
            filaPathList.add(path);
        }
        
        // Execute
        
        
        
        
    }
    
    private static void usageExit(String[] args) {
        System.out.println("Invalid args. " + args);
    }

}
