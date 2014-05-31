package ch.uzh.ddis.thesis.lambda_architecture.coordination.producer;

import ch.uzh.ddis.thesis.lambda_architecture.data.IDataEntry;
import ch.uzh.ddis.thesis.lambda_architecture.data.SRBench.SRBenchDataEntry;
import org.javatuples.Pair;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.internal.util.reflection.Whitebox;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class SystemTimeSynchronizerTest {

    @Test
    public void testRegisterDataInput() {
        SystemTimeSynchronizer<SRBenchDataEntry> synchronizer = new SystemTimeSynchronizer<SRBenchDataEntry>(Mockito.mock(KafkaProducer.class), 0, 0, -1);
        for(int i = 0; i < 10; i++){
            Assert.assertEquals(synchronizer.registerDataInput(), i);
        }
    }

    @Test
    public void testRemoveDataInput() {
        SystemTimeSynchronizer<SRBenchDataEntry> synchronizer = new SystemTimeSynchronizer<SRBenchDataEntry>(Mockito.mock(KafkaProducer.class), 0, 0, -1);
        int queueId = synchronizer.registerDataInput();

        synchronizer.removeDataInput(queueId);
        ArrayList<Boolean> removeQueue = (ArrayList<Boolean>) Whitebox.getInternalState(synchronizer, "removeQueue");
        Assert.assertTrue(removeQueue.get(queueId));
    }

    @Test
    public void testAddData() throws Exception {

    }

    @Test
    public void testRun() throws InterruptedException{
        final ArrayList<Pair<Long, Long>> results = new ArrayList<>();
        KafkaProducer<TestData> producer = Mockito.mock(KafkaProducer.class);

        Mockito.doAnswer((new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                TestData data = (TestData)invocation.getArguments()[0];
                Pair<Long, Long> pair = new Pair<>(System.currentTimeMillis(), data.getTimestamp());
                results.add(pair);

                return null;
            }
        })).when(producer).send(Mockito.any(TestData.class));

        final long systemTimeStart = System.currentTimeMillis() + 1000;
        long ticksPerMs = 1000;
        final SystemTimeSynchronizer<TestData> synchronizer = new SystemTimeSynchronizer<>(producer, systemTimeStart, ticksPerMs, -1);

        ExecutorService executor = Executors.newFixedThreadPool(5);

        for(int i = 0; i < 4; i++){
            synchronizer.registerDataInput();
        }

        executor.execute(synchronizer);

        for(int i = 0; i < 4; i++){
            final int id = i;
            Runnable dataWriter = new Runnable() {
                @Override
                public void run() {
                    for(int i = 1; i < 100; i++){
                        TestData data = new TestData(systemTimeStart + (i * (1000 * 60)));
                        synchronizer.addData(id, data);
                    }

                    synchronizer.removeDataInput(id);
                }
            };
            executor.execute(dataWriter);
        }



        executor.shutdown();
        executor.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);


        long firstData = systemTimeStart + (1 * (1000 * 60));
        for(Pair<Long, Long> result : results){
            long sys = result.getValue0();
            long data = result.getValue1();


            long sysdiff = sys - systemTimeStart;
            long datadiff = data - firstData;

            System.out.println((sysdiff * ticksPerMs) + " " + datadiff);

            Assert.assertTrue((sysdiff * ticksPerMs) >= datadiff);
        }
    }



    private class TestData implements IDataEntry{
        private final long time;

        public TestData(long time){
            this.time = time;
        }

        @Override
        public String getId() {
            return String.valueOf(time);
        }

        @Override
        public String getPartitionKey() {
            return "";
        }

        @Override
        public long getTimestamp() {
            return this.time;
        }

        @Override
        public String getTopic() {
            return "";
        }
    }

}