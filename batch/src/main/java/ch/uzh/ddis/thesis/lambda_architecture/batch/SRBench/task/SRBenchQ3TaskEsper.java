package ch.uzh.ddis.thesis.lambda_architecture.batch.SRBench.task;

import ch.uzh.ddis.thesis.lambda_architecture.batch.EsperUpdateListener;
import ch.uzh.ddis.thesis.lambda_architecture.batch.RecoverableSamzaTask;
import ch.uzh.ddis.thesis.lambda_architecture.batch.SRBench.SRBenchDataEntry;
import ch.uzh.ddis.thesis.lambda_architecture.batch.cache.TimeWindowCache;
import ch.uzh.ddis.thesis.lambda_architecture.batch.cache.TumblingWindowCache;
import ch.uzh.ddis.thesis.lambda_architecture.batch.serde.GenericData;
import com.espertech.esper.client.*;
import com.espertech.esper.client.time.CurrentTimeEvent;
import org.apache.samza.config.Config;
import org.apache.samza.storage.kv.Entry;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.TaskContext;
import org.apache.samza.task.TaskCoordinator;
import org.javatuples.Pair;

import java.util.Iterator;
import java.util.List;

/**
 * Stream Task to answer SRBench Question 1 using esper engine:
 * `Get the rainfall observed once in an hour`
 *
 * @author Nicolas Baer <nicolas.baer@gmail.com>
 */
public final class SRBenchQ3TaskEsper extends RecoverableSamzaTask {
    private static final String esperEngineName = "srbench-q3";
    private EPRuntime esper;
    private TimeWindowCache<SRBenchDataEntry> timeWindowCache;
    private EsperUpdateListener esperUpdateListener;

    private final SystemStream resultStream = new SystemStream("kafka", "srbench-q3-results");

    @Override
    public void init(Config config, TaskContext taskContext) throws Exception {
        super.init(config, taskContext);
        this.timeWindowCache = new TumblingWindowCache<>(super.genericStore, 60l * 60l * 1000l);
        this.initEsper();
    }

    @Override
    protected void processMessage(IncomingMessageEnvelope incomingMessageEnvelope, MessageCollector messageCollector, TaskCoordinator taskCoordinator, boolean possibleRestore) {

        String message = (String) incomingMessageEnvelope.getMessage();
        SRBenchDataEntry entry = new SRBenchDataEntry(message);

        if(possibleRestore){
            Iterator<Entry<String, GenericData>> it = this.timeWindowCache.retrieve();
            while(it.hasNext()){
                List<SRBenchDataEntry> cachedEntries = (List<SRBenchDataEntry>) it.next().getValue().getData();
                for (SRBenchDataEntry cachedEntry : cachedEntries){
                    CurrentTimeEvent timeEvent = new CurrentTimeEvent(entry.getTimestamp());
                    this.esper.sendEvent(timeEvent);
                    this.esper.sendEvent(entry);
                }
            }
        }

        CurrentTimeEvent timeEvent = new CurrentTimeEvent(entry.getTimestamp());
        this.esper.sendEvent(timeEvent);
        this.esper.sendEvent(entry);


        if(this.esperUpdateListener.hasNewData()){
            Pair<EventBean[], EventBean[]> eventDataTouple = this.esperUpdateListener.getNewData();
            EventBean[] newEvents = eventDataTouple.getValue0();

            for(int i = 0; i < newEvents.length; i++){
                String station = (String) newEvents[i].get("station");
                String value = String.valueOf(newEvents[i].get("speed"));

                String result = new StringBuilder()
                        .append(station)
                        .append(",")
                        .append(value)
                        .append(",")
                        .append(this.timeWindowCache.getStartTime())
                        .append(",")
                        .append(this.timeWindowCache.getEndTime())
                        .toString();

                OutgoingMessageEnvelope resultMessage = new OutgoingMessageEnvelope(resultStream, result);
                messageCollector.send(resultMessage);
            }
        }

        this.timeWindowCache.cache(entry);
    }


    /**
     * Initializes the esper engine.
     */
    public void initEsper(){
        Configuration config = new Configuration();

        // enables esper to work on timestamp of event instead of system time
        config.getEngineDefaults().getThreading().setInternalTimerEnabled(false);

        config.addEventType("srbench", SRBenchDataEntry.class.getName());
        EPServiceProvider cep = EPServiceProviderManager.getProvider(esperEngineName, config);
        EPAdministrator cepAdm = cep.getEPAdministrator();
        EPStatement cepStatement = cepAdm.createEPL("" +
                "select " +
                "   station, avg(doubleValue) as speed " +
                "from " +
                "   srbench.win:time_batch(1 hour) " +
                "where " +
                "   measurement = \"WindSpeed\" " +
                "group by station " +
                "having avg(doubleValue) > 74");

        this.esperUpdateListener = new EsperUpdateListener();
        cepStatement.addListener(this.esperUpdateListener);
        this.esper = cep.getEPRuntime();
    }
}
