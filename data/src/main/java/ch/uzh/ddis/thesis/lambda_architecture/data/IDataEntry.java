package ch.uzh.ddis.thesis.lambda_architecture.data;

import java.util.Map;

/**
 * Defines the type of a data entry. A data entry represents one message from the data set.
 *
 * @author Nicolas Baer <nicolas.baer@gmail.com>
 */
public interface IDataEntry extends Partitioned, Timestamped, Identifiable, Topic {

    public void init(String csvLine);

    public Map<String, Object> getMap();

    public String getTypeStr();
}
