package ch.uzh.ddis.thesis.lambda_architecture.kafka;

import ch.uzh.ddis.thesis.lambda_architecture.kafka.producer.CSVAdaptor;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author Nicolas Baer <nicolas.baer@gmail.com>
 */
public class Main {
    private final static Logger LOGGER = Logger.getLogger(CSVAdaptor.class.getName());



    public static void main(String[] args) {
        try{
            URI uri = Main.class.getClassLoader().getResource("data/debs/first30k.csv").toURI();
            File csv = new File(uri);
            CSVAdaptor producerThread = new CSVAdaptor("debs-data", csv);
            producerThread.start();




        } catch (URISyntaxException ex){
            LOGGER.log(Level.SEVERE, "could not load csv file! %s", ex.getMessage());
        }



    }

}
