package ch.uzh.ddis.thesis.lambda_architecture.data;

import java.util.Map;

/**
 * @author Nicolas Baer <nicolas.baer@gmail.com>
 */
public interface IDataEntry extends Partitioned, Timestamped, Identifiable, Topic {

    public void init(String csvLine);

    public Map<String, Object> getMap();

}
