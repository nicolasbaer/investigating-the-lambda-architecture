package ch.uzh.ddis.thesis.lambda_architecture.data.SRBench;

import ch.uzh.ddis.thesis.lambda_architecture.data.IDataEntry;
import ch.uzh.ddis.thesis.lambda_architecture.data.IDataFactory;
import com.lmax.disruptor.EventFactory;

/**
 * @author Nicolas Baer <nicolas.baer@gmail.com>
 */
public class SRBenchDataFactory implements IDataFactory, EventFactory<IDataEntry>{

    @Override
    public SRBenchDataEntry makeDataEntryFromCSV(String csvEntry) {
        return new SRBenchDataEntry(csvEntry);
    }

    @Override
    public IDataEntry newInstance() {
        return new SRBenchDataEntry();
    }
}
