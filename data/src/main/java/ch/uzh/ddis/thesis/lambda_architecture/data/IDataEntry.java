package ch.uzh.ddis.thesis.lambda_architecture.data;

/**
 * @author Nicolas Baer <nicolas.baer@gmail.com>
 */
public interface IDataEntry extends Partitioned, Timestamped, Identifiable, Topic {

    public void init(String csvLine);

}
