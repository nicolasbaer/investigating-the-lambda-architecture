package ch.uzh.ddis.thesis.lambda_architecture.data.serde;

import java.io.Serializable;

/**
 * @author Nicolas Baer <nicolas.baer@gmail.com>
 */
public class GenericData<E extends Serializable> implements Serializable {

    private E data;

    public GenericData(E data){
        this.data = data;
    }


    public E getData() {
        return data;
    }

    public void setData(E data) {
        this.data = data;
    }
}
