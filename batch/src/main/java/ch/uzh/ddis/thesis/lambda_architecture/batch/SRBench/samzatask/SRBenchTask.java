package ch.uzh.ddis.thesis.lambda_architecture.batch.SRBench.samzatask;

import ch.uzh.ddis.thesis.lambda_architecture.batch.SRBench.SRBenchDataEntry;
import ch.uzh.ddis.thesis.lambda_architecture.batch.cache.MessageDeliveryGuaranteeCache;
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
public class SRBenchTask implements StreamTask, InitableTask{
    private final static String STORE = "mystore";
    private final static Logger logger = LogManager.getLogger();

    private KeyValueStore<String, GenericData> genericStore;
    private MessageDeliveryGuaranteeCache messageDeliveryGuarantee;
    private boolean beginning = true;

    @Override
    public void init(Config config, TaskContext taskContext) throws java.lang.Exception {
        this.genericStore = (KeyValueStore) taskContext.getStore(STORE);
        this.messageDeliveryGuarantee = new MessageDeliveryGuaranteeCache(genericStore, Integer.MAX_VALUE);
    }


    @Override
    public void process(IncomingMessageEnvelope incomingMessageEnvelope, MessageCollector messageCollector, TaskCoordinator taskCoordinator) {
        // determine if this job might have to restore to a previous state
        boolean possibleRestore = false;
        if(beginning){
            if(this.genericStore.all().hasNext()){
                possibleRestore = true;
            }
            beginning = false;
        }

        String message = (String) incomingMessageEnvelope.getMessage();
        SRBenchDataEntry entry = new SRBenchDataEntry(message);

        // samza offers at least once message delivery guarantee,
        // therefore only once message delivery has to be implemented here
        boolean alreadyProcessed = messageDeliveryGuarantee.checkAndCache(entry);

        if(!alreadyProcessed){

        }
    }
}
