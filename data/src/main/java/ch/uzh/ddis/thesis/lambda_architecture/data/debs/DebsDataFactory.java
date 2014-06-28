package ch.uzh.ddis.thesis.lambda_architecture.data.debs;

import ch.uzh.ddis.thesis.lambda_architecture.data.IDataEntry;
import ch.uzh.ddis.thesis.lambda_architecture.data.IDataFactory;
import com.lmax.disruptor.EventFactory;

import java.io.Serializable;


/**
 * @author Nicolas Baer <nicolas.baer@gmail.com>
 */
public class DebsDataFactory implements IDataFactory, EventFactory<IDataEntry>, Serializable{
    static final long serialVersionUID = 42L;

    @Override
    public DebsDataEntry makeDataEntryFromCSV(String csvEntry) {
        return new DebsDataEntry(csvEntry);
    }

    @Override
    public IDataEntry newInstance() {
        return new DebsDataEntry();
    }
}
