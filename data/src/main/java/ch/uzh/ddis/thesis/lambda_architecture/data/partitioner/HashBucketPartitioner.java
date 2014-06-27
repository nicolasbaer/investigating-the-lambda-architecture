package ch.uzh.ddis.thesis.lambda_architecture.data.partitioner;

import com.google.common.base.Optional;
import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

/**
 * The hash bucket partitioner will use the hash of the key to assign the partition to a bucket with the size
 * of number of partitions. This way every key is assigned to one partition.
 *
 * @author Nicolas Baer <nicolas.baer@gmail.com>
 */
public final class HashBucketPartitioner implements Partitioner {
    private static final Logger logger = LogManager.getLogger();

    private final HashFunction hashFunction = Hashing.md5();
    private final Map<String, Integer> mapping = new HashMap<>();


    public HashBucketPartitioner(){

    }

    /**
     * Kafka needs a constructor with the properties parameter, although we do not use the properties here. If this
     * is not present, the Kafka API will throw an Exception.
     * @param properties kafka properties
     */
    public HashBucketPartitioner(VerifiableProperties properties){
        this();
    }


    @Override
    public int partition(Object key, int numPartitions) {
        if(key instanceof String) {
            String str = (String) key;

            Optional<Integer> possiblePartition = Optional.fromNullable(mapping.get(str));
            if(possiblePartition.isPresent()){
                return possiblePartition.get();
            } else{
                HashCode hashCode = hashFunction.hashString(str, StandardCharsets.UTF_8);
                int partition = Hashing.consistentHash(hashCode, numPartitions);
                mapping.put(str, partition);
                return partition;
            }
        }

        return 0;
    }

}
