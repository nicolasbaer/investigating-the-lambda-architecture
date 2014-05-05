package ch.uzh.ddis.thesis.lambda_architecture.batch;

import ch.uzh.ddis.thesis.lambda_architecture.batch.cache.OffsetCache;
import ch.uzh.ddis.thesis.lambda_architecture.batch.serde.GenericData;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.samza.config.Config;
import org.apache.samza.storage.kv.KeyValueStore;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.task.*;


/**
 * @author Nicolas Baer <nicolas.baer@gmail.com>
 */
public abstract class RecoverableSamzaTask implements StreamTask, InitableTask{
    private final static String STORE = "mystore";
    private final static Logger logger = LogManager.getLogger();

    private Config config;
    private TaskContext taskContext;

    private RestoreManager restoreManager;
    private OffsetCache offsetCache;

    protected KeyValueStore<String, GenericData> genericStore;


    private long lastOffset = 0;
    private boolean beginning = true;

    @Override
    public void init(Config config, TaskContext taskContext) throws java.lang.Exception {
        this.genericStore = (KeyValueStore) taskContext.getStore(STORE);
        this.config = config;
        this.taskContext = taskContext;

        this.restoreManager = new RestoreManager(genericStore);
        this.offsetCache = new OffsetCache(genericStore);
    }


    @Override
    public void process(IncomingMessageEnvelope incomingMessageEnvelope, MessageCollector messageCollector, TaskCoordinator taskCoordinator) {
        // determine if this job might have to restore to a previous state
        boolean possibleRestore = this.restoreManager.checkRestore();

        // in case of a restore, samza only provides at least once message delivery guarantee
        // therefore we have to check the offsets
        boolean alreadyProcessed = false;
        long currentOffset = Long.valueOf(incomingMessageEnvelope.getOffset());
        if(possibleRestore){
            alreadyProcessed = this.offsetCache.checkCache(currentOffset);
        } else{
            this.offsetCache.cache(currentOffset);
        }

        if(!alreadyProcessed){
            this.processMessage(incomingMessageEnvelope, messageCollector, taskCoordinator, possibleRestore);
        }
    }


    protected abstract void processMessage(IncomingMessageEnvelope incomingMessageEnvelope, MessageCollector messageCollector, TaskCoordinator taskCoordinator, boolean possibleRestore);


}
