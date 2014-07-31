package ch.uzh.ddis.thesis.lambda_architecture.data.debs;

import ch.uzh.ddis.thesis.lambda_architecture.data.IDataEntry;
import org.apache.commons.lang.StringUtils;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Nicolas Baer <nicolas.baer@gmail.com>
 */
public class DebsDataEntry implements IDataEntry {
    private static final long serialVersionUID = 1L;

    private long rowId;
    private long timestamp;
    private double value;
    private DebsDataTypes.Measurement type;
    private int plugId;
    private int householdId;
    private int houseId;

    private String stringRepresentation;
    private String id;

    public DebsDataEntry(String csvEntry){
        this.init(csvEntry);
    }

    public DebsDataEntry(){

    }

    private Map<String, Object> toMap(){
        Map<String, Object> map = new HashMap<>();
        map.put("id", this.id);
        map.put("timestamp", this.timestamp);
        map.put("rowId", this.rowId);
        map.put("value", this.value);
        map.put("type", this.type.name());
        map.put("plugId", this.plugId);
        map.put("householdId", this.householdId);
        map.put("houseId", this.houseId);

        return map;
    }

    @Override
    public void init(String csvEntry) {
        this.stringRepresentation = csvEntry;

        String[] line = csvEntry.split(",");
        this.rowId = Long.valueOf(line[0]);
        this.timestamp = Long.valueOf(line[1]) * 1000;
        this.value = Double.valueOf(line[2]);
        this.type = line[3].equals("0") ? DebsDataTypes.Measurement.Work : DebsDataTypes.Measurement.Load;
        this.plugId = Integer.valueOf(line[4]);
        this.householdId = Integer.valueOf(line[5]);
        this.houseId = Integer.valueOf(line[6]);

        StringBuilder idBuilder = new StringBuilder();
        this.id = idBuilder.append(StringUtils.leftPad(String.valueOf(houseId), 2, '0'))
                .append(StringUtils.leftPad(String.valueOf(householdId), 2, '0'))
                .append(StringUtils.leftPad(String.valueOf(plugId), 2, '0'))
                .toString();
    }

    @Override
    public String getId() {
        return this.id;
    }

    @Override
    public String getPartitionKey() {
        return String.valueOf(houseId);
    }

    @Override
    public long getTimestamp() {
        return this.timestamp;
    }

    @Override
    public String getTopic() {
        return this.type.name();
    }

    public long getRowId() {
        return rowId;
    }

    public double getValue() {
        return value;
    }

    public DebsDataTypes.Measurement getType() {
        return type;
    }

    public int getPlugId() {
        return plugId;
    }

    public int getHouseholdId() {
        return householdId;
    }

    public int getHouseId() {
        return houseId;
    }

    @Override
    public String toString() {
        return stringRepresentation;
    }

    public Map<String, Object> getMap() {
        return this.toMap();
    }

    @Override
    public String getTypeStr() {
        return type.name();
    }
}
