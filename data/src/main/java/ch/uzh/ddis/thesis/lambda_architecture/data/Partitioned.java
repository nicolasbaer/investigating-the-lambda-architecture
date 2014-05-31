package ch.uzh.ddis.thesis.lambda_architecture.data;

/**
 * Indicates the data object is partitioned and holds a partition key.
 * @author Nicolas Baer <nicolas.baer@gmail.com>
 */
public interface Partitioned {

    /**
     * @return partition key
     */
    public String getPartitionKey();



}
