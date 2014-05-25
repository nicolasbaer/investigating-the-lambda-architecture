package ch.uzh.ddis.thesis.lambda_architecture.batch.cache;

import ch.uzh.ddis.thesis.lambda_architecture.data.Identifiable;

/**
 * @author Nicolas Baer <nicolas.baer@gmail.com>
 */
class StringItem implements Identifiable<String> {
    private final String str;

    public StringItem(String str){
        this.str = str;
    }

    @Override
    public String getId() {
        return str;
    }
}