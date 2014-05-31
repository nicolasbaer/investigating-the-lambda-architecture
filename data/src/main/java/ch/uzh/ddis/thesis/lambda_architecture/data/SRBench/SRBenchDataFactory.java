package ch.uzh.ddis.thesis.lambda_architecture.data.SRBench;

import ch.uzh.ddis.thesis.lambda_architecture.data.IDataFactory;

/**
 * @author Nicolas Baer <nicolas.baer@gmail.com>
 */
public class SRBenchDataFactory implements IDataFactory<SRBenchDataEntry>{

    @Override
    public SRBenchDataEntry makeDataEntryFromCSV(String csvEntry) {
        return new SRBenchDataEntry(csvEntry);
    }
}
