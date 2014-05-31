package ch.uzh.ddis.thesis.lambda_architecture.data;

/**
 * @author Nicolas Baer <nicolas.baer@gmail.com>
 */
public interface IDataFactory<E extends IDataEntry> {

    public E makeDataEntryFromCSV(String csvEntry);

}
