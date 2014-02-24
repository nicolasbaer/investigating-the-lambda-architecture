package ch.uzh.ddis.thesis.lambda_architecture.kafka.producer;

import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.supercsv.cellprocessor.ParseBool;
import org.supercsv.cellprocessor.ParseDouble;
import org.supercsv.cellprocessor.ParseInt;
import org.supercsv.cellprocessor.ift.CellProcessor;
import org.supercsv.io.CsvListReader;
import org.supercsv.io.ICsvListReader;
import org.supercsv.prefs.CsvPreference;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Simple Producer example that reads data from a CSV file.
 *
 * @author Nicolas Baer <nicolas.baer@gmail.com>
 */
public final class CSVAdaptor extends Thread{
    private final static Logger LOGGER = Logger.getLogger(CSVAdaptor.class.getName());
    public final static CellProcessor[] PROCESSOR = new CellProcessor[] {
            new ParseInt(),
            new ParseInt(),
            new ParseDouble(),
            new ParseBool(),
            new ParseInt(),
            new ParseInt(),
            new ParseInt()};

    private final kafka.javaapi.producer.Producer<String, List<Object>> producer;
    private final String topic;
    private final Properties props = new Properties();
    private final File csvFile;


    /**
     * default constructor
     */
    public CSVAdaptor(final String topic, File csvFile) {
        props.put("metadata.broker.list", "localhost:9092");
        props.put("serializer.class", "ch.uzh.ddis.thesis.lambda_architecture.kafka.serializer.ListSerializer");
        props.put("key.serializer.class", "kafka.serializer.StringEncoder");
        props.put("zk.connect", "localhost:2181");

        // Use random partitioner. Don't need the key type. Just set it to Integer.
        // The message is of type String.
        producer = new kafka.javaapi.producer.Producer<>(new ProducerConfig(props));

        this.topic = topic;
        this.csvFile = csvFile;
    }

    public void run(){

        ICsvListReader listReader = null;
        try {
            listReader = new CsvListReader(new FileReader(this.csvFile), CsvPreference.STANDARD_PREFERENCE);
            listReader.getHeader(true);

            final CellProcessor[] processor = PROCESSOR;

            List<Object> row;
            while( (row = listReader.read(processor)) != null ) {
                KeyedMessage<String, List<Object>> message = new KeyedMessage<>(this.topic, row.get(0).toString(), row);
                producer.send(message);
            }
        } catch (IOException ex){
            LOGGER.log(Level.SEVERE, "CSV file not found, stopping producer: %s", this.csvFile.getAbsolutePath());
            return;
        }
        finally {
            if( listReader != null ) {
                try {
                    listReader.close();
                } catch (IOException ex){
                    LOGGER.log(Level.INFO, "could not close file properly: %s", this.csvFile.getAbsolutePath());
                }
            }
        }
    }

}
