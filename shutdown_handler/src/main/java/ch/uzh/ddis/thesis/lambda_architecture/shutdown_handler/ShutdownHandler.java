package ch.uzh.ddis.thesis.lambda_architecture.shutdown_handler;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Marker;
import org.apache.logging.log4j.MarkerManager;

import java.io.File;
import java.io.IOException;
import java.util.UUID;

/**
 * @author Nicolas Baer <nicolas.baer@gmail.com>
 */
public final class ShutdownHandler {
    private static final Logger logger = LogManager.getLogger();
    private static final Marker performance = MarkerManager.getMarker("PERFORMANCE");

    /**
     * Handles the shutdown of the speed or batch layer.
     * The main problem this method has to solve is the coordinated shutdown of
     * all parallelised threads. In case this is called on a TORQUE or SLURM cluster
     * it will write a file for each call into ~/shutdown_handler/
     *
     * In case this is called on a local machine nothing is done!
     *
     * TODO: This is a very quick workaround to the problem and fulfils the current needs.
     *       In case you find a good solution, let me know! :)
     */
    public static void handleShutdown(final String message){
        String id = UUID.randomUUID().toString();

        logger.info(performance, "topic=shutdown id={} {}", id, message);

        boolean cluster = false;

        if(new File("/home/user/baer/shutdown_handler").exists()){
            cluster = true;
        }

        if(cluster){
            try {
                new File("/home/user/baer/shutdown_handler/" + id).createNewFile();
            } catch (IOException e){
                logger.error(e);
            }
        }

    }



}
