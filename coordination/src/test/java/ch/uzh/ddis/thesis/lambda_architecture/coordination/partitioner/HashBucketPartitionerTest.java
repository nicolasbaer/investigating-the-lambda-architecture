package ch.uzh.ddis.thesis.lambda_architecture.coordination.partitioner;

import kafka.producer.Partitioner;
import org.apache.commons.lang.RandomStringUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Nicolas Baer <nicolas.baer@gmail.com>
 */
public class HashBucketPartitionerTest {

    @Test
    public void testPartitionSize(){
        Partitioner partitioner = new HashBucketPartitioner();

        for(int i = 0; i < 500; i++){
            String randomString = RandomStringUtils.random(10);
            int partition = partitioner.partition(randomString, 20);

            Assert.assertTrue(partition >= 0 && partition < 20);
        }
    }

    /**
     * The producers will be started in different threads or even on different machines.
     * Therefore the partitioner has to guarantee to deliver a deterministic result.
     */
    @Test
    public void testRepeatedPartitioning(){
        // lets run this in a loop, so chance will not get in the way :)
        for(int i = 0; i < 20; i++) {
            List<String> randomStrings = new ArrayList<>();
            for (int j = 0; j < 500; j++) {
                randomStrings.add(RandomStringUtils.random(10));
            }

            Partitioner partitioner = new HashBucketPartitioner();
            Partitioner secondPartitioner = new HashBucketPartitioner();
            for (String randomString : randomStrings) {
                int partition = partitioner.partition(randomString, 50);
                int partitionSecond = secondPartitioner.partition(randomString, 50);
                Assert.assertEquals(partition, partitionSecond);
            }
        }
    }
}
