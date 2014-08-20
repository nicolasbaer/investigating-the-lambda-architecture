package ch.uzh.ddis.thesis.lambda_architecture.data.partitioner;

import com.google.common.base.Optional;
import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

/**
 * The hash bucket partitioner will use the hash of the key to assign the partition to a bucket with the size
 * of number of partitions. This way every key is assigned to one partition.
 *
 * Consistent hashing is used to provide a unified partitioning method over all components of this architecture.
 *
 * @author Nicolas Baer <nicolas.baer@gmail.com>
 */
public final class HashBucketPartitioner implements Partitioner, Serializable {
    static final long serialVersionUID = 42L;

    private static final Logger logger = LogManager.getLogger();

    private final HashFunction hashFunction = Hashing.md5();
    private final Map<String, Integer> mapping = new HashMap<>();


    public HashBucketPartitioner(){

    }

    /**
     * Apparently, Kafka needs a constructor with the properties parameter, although we do not use the properties here. If this
     * is not present, the Kafka API will throw an Exception and may fail.
     *
     * @param properties kafka properties
     */
    public HashBucketPartitioner(VerifiableProperties properties){
        this();
    }

    /**
     * In order to partition the data consistent hashing is used. Based on the md5 hash of the key, the key will be placed
     * in one of the buckets (`numPartitions`). A local cache is kept in order to speed up the partitioning of keys.
     *
     * Note that a key has to be of the type String! However due to the design of the interfaces of Kafka we can not
     * enforce this type. If the key is not of type String, the partition will always be 0, since we can not make any
     * assumptions.
     *
     * @param key the key to partition
     * @param numPartitions the number of partitions
     * @return the partition for the key
     */
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
