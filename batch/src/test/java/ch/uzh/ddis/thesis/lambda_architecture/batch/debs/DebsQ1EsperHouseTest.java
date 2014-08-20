package ch.uzh.ddis.thesis.lambda_architecture.batch.DEBS;

import org.junit.Test;

public class DebsQ1EsperHouseTest {

    @Test
    public void testRound(){


        System.out.println(round(0.001, 5));




    }


    public Double round(final Double number, final int decimals){
        double dec = 1 * Math.pow(10, decimals);
        return Math.round(number.doubleValue() * dec) / dec;
    }

}