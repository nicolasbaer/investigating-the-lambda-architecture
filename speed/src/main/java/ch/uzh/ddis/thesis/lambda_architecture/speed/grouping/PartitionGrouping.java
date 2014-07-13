package ch.uzh.ddis.thesis.lambda_architecture.speed.grouping;

import backtype.storm.generated.GlobalStreamId;
import backtype.storm.grouping.CustomStreamGrouping;
import backtype.storm.task.WorkerTopologyContext;
import ch.uzh.ddis.thesis.lambda_architecture.data.partitioner.HashBucketPartitioner;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Nicolas Baer <nicolas.baer@gmail.com>
 */
public class PartitionGrouping implements CustomStreamGrouping{

    private HashBucketPartitioner partitioner;
    private List<Integer> targetTasks;
    private int taskNum;

    @Override
    public void prepare(WorkerTopologyContext context, GlobalStreamId stream, List<Integer> targetTasks) {
        this.partitioner = new HashBucketPartitioner();
        this.targetTasks = targetTasks;
        this.taskNum = targetTasks.size();
    }

    @Override
    public List<Integer> chooseTasks(int taskId, List<Object> values) {
        List<Integer> forwardTasks = new ArrayList<>(1);

        int partition = this.partitioner.partition(String.valueOf(values.get(1)), this.taskNum);

        forwardTasks.add(this.targetTasks.get(partition));

        return forwardTasks;

    }
}
