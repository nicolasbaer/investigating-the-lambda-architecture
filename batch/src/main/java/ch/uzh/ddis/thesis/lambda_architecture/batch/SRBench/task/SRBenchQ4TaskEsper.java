package ch.uzh.ddis.thesis.lambda_architecture.batch.SRBench.task;

import ch.uzh.ddis.thesis.lambda_architecture.data.esper.EsperUpdateListener;
import ch.uzh.ddis.thesis.lambda_architecture.data.SRBench.SRBenchDataEntry;
import ch.uzh.ddis.thesis.lambda_architecture.batch.time_window.TimeWindow;
import ch.uzh.ddis.thesis.lambda_architecture.batch.time_window.TumblingWindow;
import com.espertech.esper.client.*;
import com.espertech.esper.client.time.CurrentTimeEvent;
import org.apache.samza.config.Config;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.*;
import org.javatuples.Pair;

/**
 * Stream Task to answer SRBench Question 1 using esper engine:
 * `Get the rainfall observed once in an hour`
 *
 * @author Nicolas Baer <nicolas.baer@gmail.com>
 */
public final class SRBenchQ4TaskEsper implements StreamTask, InitableTask {
    private static final String esperEngineName = "srbench-q4";
    private EPRuntime esper;
    private TimeWindow<SRBenchDataEntry> timeWindow;
    private EsperUpdateListener esperUpdateListener;

    private final SystemStream resultStream = new SystemStream("kafka", "srbench-q4-results");

    @Override
    public void init(Config config, TaskContext taskContext) throws Exception {
        long windowSize = 60l * 60l * 1000l;
        this.timeWindow = new TumblingWindow<>(windowSize);
        this.initEsper();
    }

    @Override
    public void process(IncomingMessageEnvelope incomingMessageEnvelope, MessageCollector messageCollector, TaskCoordinator taskCoordinator) {
        String message = (String) incomingMessageEnvelope.getMessage();
        SRBenchDataEntry entry = new SRBenchDataEntry(message);

        if(!this.timeWindow.isInWindow(entry)){

            CurrentTimeEvent timeEvent = new CurrentTimeEvent(entry.getTimestamp());
            this.esper.sendEvent(timeEvent);
            this.esper.sendEvent(entry);

            if(this.esperUpdateListener.hasNewData()){
                Pair<EventBean[], EventBean[]> eventDataTouple = this.esperUpdateListener.getNewData();
                EventBean[] newEvents = eventDataTouple.getValue0();

                for(int i = 0; i < newEvents.length; i++){
                    String station = (String) newEvents[i].get("station");
                    String value = (String) newEvents[i].get("value");
                    String unit = (String) newEvents[i].get("unit");

                    String result = new StringBuilder()
                            .append(station)
                            .append(",")
                            .append(value)
                            .append(",")
                            .append(unit)
                            .append(",")
                            .append(this.timeWindow.getWindowStart())
                            .append(",")
                            .append(this.timeWindow.getWindowEnd())
                            .toString();

                    OutgoingMessageEnvelope resultMessage = new OutgoingMessageEnvelope(resultStream, result);
                    messageCollector.send(resultMessage);
                }
            }
        } else{
            CurrentTimeEvent timeEvent = new CurrentTimeEvent(entry.getTimestamp());
            this.esper.sendEvent(timeEvent);
            this.esper.sendEvent(entry);
        }

        this.timeWindow.addEvent(entry);
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
                "   station, value, unit " +
                "from " +
                "   srbench.win:time_batch(1 hour) " +
                "where " +
                "   observation = \"RainfallObservation\" " +
                "output " +
                "   every 1 hour and when terminated");

        this.esperUpdateListener = new EsperUpdateListener();
        cepStatement.addListener(this.esperUpdateListener);
        this.esper = cep.getEPRuntime();
    }
}
