package ch.uzh.ddis.thesis.lambda_architecture.kafka.producer;

import au.com.bytecode.opencsv.CSVReader;
import com.google.gson.Gson;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Simple Producer example that reads data from a CSV file.
 *
 * @author Nicolas Baer <nicolas.baer@gmail.com>
 */
public final class CSVAdaptor implements Runnable{
    private final static Logger LOGGER = Logger.getLogger(CSVAdaptor.class.getName());

    private final kafka.javaapi.producer.Producer<String, String> producer;
    private final String topic;
    private final Properties props;
    private final File csvFile;


    /**
     * default constructor
     */
    public CSVAdaptor(final String topic, final File csvFile, final Properties props) {
        producer = new kafka.javaapi.producer.Producer<>(new ProducerConfig(props));
        this.topic = topic;
        this.csvFile = csvFile;
        this.props = props;
    }

    /**
     * Publishes the csv to the kafka topic line-by-line.
     */
    private void publishCsv(){
        CSVReader reader = null;

        try {
            reader = new CSVReader(new FileReader(this.csvFile));
            String [] line;
            while ((line = reader.readNext()) != null) {
                String json = new Gson().toJson(line);
                KeyedMessage<String, String> message = new KeyedMessage<>(this.topic, line[0].toString(), json);
                producer.send(message);
            }
        } catch (IOException ex){
            LOGGER.log(Level.SEVERE, "CSV file not found, stopping producer: %s", this.csvFile.getAbsolutePath());
            return;
        }
        finally {
            if(reader != null){
                try {
                    reader.close();
                } catch (IOException ex){
                    LOGGER.log(Level.INFO, "could not close file properly: %s", this.csvFile.getAbsolutePath());
                }
            }
        }
    }


    public void run(){
        this.publishCsv();
    }

}
