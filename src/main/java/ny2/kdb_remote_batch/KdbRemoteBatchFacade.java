package ny2.kdb_remote_batch;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kx.c;
import kx.c.KException;

/**
 * KdbRemoteBatchFacade
 */
public class KdbRemoteBatchFacade {

    // //////////////////////////////////////
    // Filed
    // //////////////////////////////////////

    private static final Logger log = LoggerFactory.getLogger(KdbRemoteBatchFacade.class);
    
    // kdb
    private String host;
    private int port;
    private String username;
    private String password;
    private boolean useTLS;

    /** kdb+ connector */
    private c c;

    // //////////////////////////////////////
    // Constructor
    // //////////////////////////////////////

    public KdbRemoteBatchFacade(String kdbHandle) throws Exception {
        log.info("initialize kdb connection. {}", kdbHandle);
        parseKdbHandle(kdbHandle);
    }

    // //////////////////////////////////////
    // Method
    // //////////////////////////////////////

    public void executeRemoteBatch(List<Path> qfilePathList) throws Exception {
        // send query
        for (Path qfilePath : qfilePathList) {
            log.info("Send query. file={}", qfilePath);
            List<String> lines = Files.readAllLines(qfilePath);
            String query = String.join("\n", lines);
            c.k(query);

            TimeUnit.SECONDS.sleep(1);
        }

        // close
        try {
            c.close();
        } catch (Exception e) {
        }
    }

    /**
     * Parse kdb handle. `:host:port[:user:password]
     * 
     * @param kdbHandle
     * @throws IOException
     * @throws KException
     */
    private void parseKdbHandle(String kdbHandle) throws Exception {
        String[] items = kdbHandle.split(":");
        if (items.length < 3) {
            return;
        }
        this.host = StringUtils.trimToEmpty(items[1]);
        this.port = NumberUtils.toInt(items[2]);
        if (items.length >= 5) {
            this.username = StringUtils.trimToEmpty(items[3]);
            this.password = StringUtils.trimToEmpty(items[4]);
        }

        if (StringUtils.isBlank(username)) {
            this.c = new kx.c(host, port);
        } else {
            this.c = new kx.c(host, port, username + ":" + password);
        }
        log.info("open kdb connection. {}", c);
    }

    // //////////////////////////////////////
    // Method - Convert
    // //////////////////////////////////////

}
