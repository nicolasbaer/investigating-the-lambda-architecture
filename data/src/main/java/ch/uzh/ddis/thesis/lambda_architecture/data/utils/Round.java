package ch.uzh.ddis.thesis.lambda_architecture.data.utils;

/**
 * Implements a simple round function in order to provide one unified method for all tasks.
 *
 * @author Nicolas Baer <nicolas.baer@gmail.com>
 */
public final class Round {

    private final static double dec = 1 * Math.pow(10, 5);

    /**
     * Rounds a double to the maximum of five decimals
     * @param number number to round
     * @return
     */
    public static Double roundToFiveDecimals(final Double number){

        if(number == null){
            return null;
        }

        return Math.round(number.doubleValue() * dec) / dec;
    }


}
