package ch.uzh.ddis.thesis.lambda_architecture.batch.SRBench.task;

import ch.uzh.ddis.thesis.lambda_architecture.batch.cache.HashKV;
import ch.uzh.ddis.thesis.lambda_architecture.data.serde.GenericData;
import ch.uzh.ddis.thesis.lambda_architecture.data.serde.GenericSerde;
import org.apache.samza.Partition;
import org.apache.samza.config.MapConfig;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.serializers.StringSerde;
import org.apache.samza.storage.kv.KeyValueStore;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.TaskContext;
import org.apache.samza.task.TaskCoordinator;
import org.junit.Assert;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;

public class SRBenchQ3TaskEsperTest {

    @Test
    public void testProcessMessage() throws Exception {

        SRBenchQ3TaskEsper task = new SRBenchQ3TaskEsper();
        task.init(new MapConfig(), new TaskContextMock());


        System.out.println(getClass().getClassLoader().getResource("bill_sample.csv"));
        String csvPath = getClass().getClassLoader().getResource("bill_sample.csv").getPath();
        File csv = new File(csvPath);

        TaskCoordinator coordinator = new TaskCoordinatorMock();

        final ArrayList<String> results = new ArrayList<>();

        MessageCollector collector = new MessageCollector() {
            @Override
            public void send(OutgoingMessageEnvelope outgoingMessageEnvelope) {
                results.add((String) outgoingMessageEnvelope.getMessage());
            }
        };


        BufferedReader reader = new BufferedReader(new FileReader(csv));
        String line = null;
        int i = 0;
        while((line = reader.readLine()) != null){


            IncomingMessageEnvelope message = new IncomingMessageEnvelope(new SystemStreamPartition("test", "test", new Partition(0)), String.valueOf(i), new String("test"), line);

            task.process(message, collector, coordinator);

            i++;
        }

        for(String result : results){
            System.out.println(result);
        }


    }


    private class TaskCoordinatorMock implements TaskCoordinator{
        @Override
        public void commit() {
            //no-op
        }

        @Override
        public void shutdown() {
            //no-op
        }

        @Override
        public void shutdown(ShutdownMethod shutdownMethod) {
            //no-op
        }
    }

    private class TaskContextMock implements TaskContext{
        @Override
        public MetricsRegistry getMetricsRegistry() {
            return null;
        }

        @Override
        public Partition getPartition() {
            return null;
        }

        @Override
        public Object getStore(String s) {
            KeyValueStore<String, GenericData> store = null;
            try {
                store = new HashKV<>(new StringSerde(StandardCharsets.UTF_8.toString()), new GenericSerde());
            } catch (IOException e){
                Assert.assertTrue(false);
            }
            return store;
        }
    }
}