package ch.uzh.ddis.thesis.lambda_architecture.data.SRBench;

import ch.uzh.ddis.thesis.lambda_architecture.data.IDataEntry;
import org.apache.commons.lang.math.NumberUtils;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Represents one entry within the SRBench Dataset.
 *
 * @author Nicolas Baer <nicolas.baer@gmail.com>
 */
public final class SRBenchDataEntry implements IDataEntry, Serializable{

    private final long timestamp;
    private final String station;
    private final String measurement;
    private final String value;
    private final String unit;
    private final String observation;
    private final String id;
    private final SRBenchDataTypes.Observation observationType;
    private final SRBenchDataTypes.Measurement measurementType;
    private final Map<String, Object> map;


    public SRBenchDataEntry(String csvEntry){
        String[] line = csvEntry.split(",");

        this.timestamp = Long.valueOf(line[0]) * 1000;
        this.station = line[1];
        this.measurement = line[2];
        this.value = line[3];
        this.unit = line[4];
        this.observation = line[5];

        this.observationType = SRBenchDataTypes.Observation.valueOf(observation);
        this.measurementType = SRBenchDataTypes.Measurement.valueOf(measurement);

        StringBuilder idBuilder = new StringBuilder();
        this.id = idBuilder.append(timestamp).append(station).append(observation).append(measurement).toString();

        this.map = this.toMap();
    }


    private Map<String, Object> toMap(){
        Map<String, Object> map = new HashMap<>();
        map.put("timestamp", this.timestamp);
        map.put("station", this.station);
        map.put("measurement", this.measurementType.name());
        if(NumberUtils.isNumber(this.value)){
            map.put("value", Double.valueOf(this.value));
        } else{
            map.put("value", Boolean.valueOf(this.value));
        }

        map.put("unit", this.unit);
        map.put("observation", this.observationType.name());

        return map;
    }

    @Override
    public String getTopic() {
        return observationType.name();
    }

    @Override
    public String getPartitionKey() {
        return station;
    }


    public long getTimestamp() {
        return timestamp;
    }

    public String getStation() {
        return station;
    }

    public String getMeasurement() {
        return measurement;
    }

    public String getValue() {
        return value;
    }

    public String getUnit() {
        return unit;
    }

    public String getObservation() {
        return observation;
    }

    public String getId() {
        return id;
    }

    public SRBenchDataTypes.Observation getObservationType() {
        return observationType;
    }

    public SRBenchDataTypes.Measurement getMeasurementType() {
        return measurementType;
    }

    public Map<String, Object> getMap() {
        return map;
    }
}
