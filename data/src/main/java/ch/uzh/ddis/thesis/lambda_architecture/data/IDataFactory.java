package ch.uzh.ddis.thesis.lambda_architecture.data;

import com.lmax.disruptor.EventFactory;

/**
 * @author Nicolas Baer <nicolas.baer@gmail.com>
 */
public interface IDataFactory extends EventFactory<IDataEntry>{

    public IDataEntry makeDataEntryFromCSV(String csvEntry);

}
