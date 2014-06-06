package ch.uzh.ddis.thesis.lambda_architecture.data.debs;

import ch.uzh.ddis.thesis.lambda_architecture.data.IDataFactory;

/**
 * @author Nicolas Baer <nicolas.baer@gmail.com>
 */
public class DebsDataFactory implements IDataFactory<DebsDataEntry>{

    @Override
    public DebsDataEntry makeDataEntryFromCSV(String csvEntry) {
        return new DebsDataEntry(csvEntry);
    }
}
