package ch.uzh.ddis.thesis.lambda_architecture.batch.SRBench;

import ch.uzh.ddis.thesis.lambda_architecture.batch.cache.Identifiable;
import ch.uzh.ddis.thesis.lambda_architecture.batch.cache.Timestamped;

/**
 * Represents one entry within the SRBench Dataset.
 * 1253570400,HPROS,WindGust,3.0,milesPerHour,WindObservation
 *
 * @author Nicolas Baer <nicolas.baer@gmail.com>
 */
public class SRBenchDataEntry implements Timestamped, Identifiable<String>{

    private long timestamp;
    private String station;
    private String measurement;
    private String value;
    private String unit;
    private String observation;
    private String id;

    public SRBenchDataEntry(String csvEntry){
        String[] line = csvEntry.split(",");

        this.timestamp = new Long(line[0]);
        this.station = line[1];
        this.measurement = line[2];
        this.value = line[3];
        this.unit = line[4];
        this.observation = line[5];

        StringBuilder idBuilder = new StringBuilder();
        this.id = idBuilder.append(timestamp).append(station).append(observation).append(measurement).toString();

    }


    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public String getStation() {
        return station;
    }

    public void setStation(String station) {
        this.station = station;
    }

    public String getMeasurement() {
        return measurement;
    }

    public void setMeasurement(String measurement) {
        this.measurement = measurement;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public String getUnit() {
        return unit;
    }

    public void setUnit(String unit) {
        this.unit = unit;
    }

    public String getObservation() {
        return observation;
    }

    public void setObservation(String observation) {
        this.observation = observation;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }
}
