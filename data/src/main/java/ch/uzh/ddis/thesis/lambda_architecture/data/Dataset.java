package ch.uzh.ddis.thesis.lambda_architecture.data;

import ch.uzh.ddis.thesis.lambda_architecture.data.SRBench.SRBenchDataFactory;
import ch.uzh.ddis.thesis.lambda_architecture.data.debs.DebsDataFactory;

/**
 * @author Nicolas Baer <nicolas.baer@gmail.com>
 */
public enum Dataset {

    srbench(){
        @Override
        public IDataFactory getFactory(){
            return new SRBenchDataFactory();
        }
    }, debs(){
        @Override
        public IDataFactory getFactory(){
            return new DebsDataFactory();
        }
    };

    public abstract IDataFactory getFactory();
}
