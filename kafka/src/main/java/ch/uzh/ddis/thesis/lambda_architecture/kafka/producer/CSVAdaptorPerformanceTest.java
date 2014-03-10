package ch.uzh.ddis.thesis.lambda_architecture.kafka.producer;

import au.com.bytecode.opencsv.CSVReader;
import com.ecyrd.speed4j.StopWatch;
import com.google.gson.Gson;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Marker;
import org.apache.logging.log4j.MarkerManager;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Simple Producer example that reads data from a CSV file.
 *
 * @author Nicolas Baer <nicolas.baer@gmail.com>
 */
public final class CSVAdaptorPerformanceTest implements Runnable{
    private static final Logger logger = LogManager.getLogger();
    private static final Marker performance = MarkerManager.getMarker("PERFORMANCE");

    private final kafka.javaapi.producer.Producer<String, String> producer;
    private final String topic;
    private final Properties props;
    private final File csvFile;
    private final int batchSize;


    /**
     * default constructor
     */
    public CSVAdaptorPerformanceTest(final String topic, final File csvFile, final Properties props, final int batchSize) {
        producer = new kafka.javaapi.producer.Producer<>(new ProducerConfig(props));
        this.topic = topic;
        this.csvFile = csvFile;
        this.props = props;
        this.batchSize = batchSize;
    }

    /**
     * Publishes the csv to the kafka topic line-by-line.
     */
    private void publishCsv(){
        CSVReader reader = null;

        try {
            StopWatch watch = new StopWatch("kafka_csv_writeline_json");
            reader = new CSVReader(new FileReader(this.csvFile));
            String[] line;
            List<KeyedMessage<String, String>> messages = new ArrayList<>();
            int i = 0;
            while ((line = reader.readNext()) != null) {
                String json = new Gson().toJson(line);
                KeyedMessage<String, String> message = new KeyedMessage<>(this.topic, line[0].toString(), json);
                messages.add(message);

                if(messages.size() % batchSize == 0) {
                    producer.send(messages);
                    messages.clear();
                }

                if(i % 1000 == 0 && i != 0) {
                    watch.stop();
                    logger.info(performance, "step={} topic={} batchSize={} duration={}", i, watch.getTag(), batchSize, watch.getTimeMicros());
                    watch = new StopWatch("kafka_csv_writeline_json");
                }
                i++;

                if(i > 5000){
                    break;
                }

            }
            if (!messages.isEmpty()) {
                producer.send(messages);
            }
        } catch (IOException ex) {
            logger.error("CSV file not found, stopping producer: {}", this.csvFile.getAbsolutePath());
            return;
        }
        finally {
            if(reader != null){
                try {
                    reader.close();
                } catch (IOException ex){
                    logger.error("could not close file properly: {}", this.csvFile.getAbsolutePath());
                }
            }
        }
    }


    public void run(){
        this.publishCsv();
    }

}
