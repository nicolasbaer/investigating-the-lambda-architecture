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

    private final long rowId;
    private final long timestamp;
    private final double value;
    private final DebsDataTypes.Measurement type;
    private final int plugId;
    private final int householdId;
    private final int houseId;

    private final String stringRepresentation;
    private final Map<String, Object> map;
    private final String id;

    public DebsDataEntry(String csvEntry){
        this.stringRepresentation = csvEntry;

        String[] line = csvEntry.split(",");
        this.rowId = Long.valueOf(line[0]);
        this.timestamp = Long.valueOf(line[1]) * 1000;
        this.value = Double.valueOf(line[2]);
        this.type = line[3].equals("0") ? DebsDataTypes.Measurement.Work : DebsDataTypes.Measurement.Load;
        this.plugId = Integer.valueOf(line[4]);
        this.householdId = Integer.valueOf(line[5]);
        this.houseId = Integer.valueOf(line[6]);

        this.map = toMap();
        StringBuilder idBuilder = new StringBuilder();
        this.id = idBuilder.append(StringUtils.leftPad(String.valueOf(houseId), 2, '0'))
                .append(StringUtils.leftPad(String.valueOf(householdId), 2, '0'))
                .append(StringUtils.leftPad(String.valueOf(plugId), 2, '0'))
                .toString();
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

    public String getStringRepresentation() {
        return stringRepresentation;
    }

    public Map<String, Object> getMap() {
        return map;
    }
}
