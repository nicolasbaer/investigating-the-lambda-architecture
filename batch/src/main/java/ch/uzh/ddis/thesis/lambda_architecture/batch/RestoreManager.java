package ch.uzh.ddis.thesis.lambda_architecture.batch;

import ch.uzh.ddis.thesis.lambda_architecture.data.serde.GenericData;
import org.apache.samza.storage.kv.KeyValueStore;

/**
 * This class is depricated as it was used to provide an alternative checkpoint solution for Samza.
 *
 * @author Nicolas Baer <nicolas.baer@gmail.com>
 */
@Deprecated
public class RestoreManager {
    private final KeyValueStore<String, GenericData> store;
    private boolean beginning;

    public RestoreManager(KeyValueStore<String, GenericData> store){
        this.store = store;
        this.beginning = true;
    }


    /**
     * Checks whether the task has to restore data.
     * @return true if it has to restore
     */
    public boolean checkRestore(){
        boolean restore = false;
        if(this.beginning){
            if(this.store.all().hasNext()){
                restore = true;
            }
            this.beginning = false;
        }

        return restore;
    }
}
