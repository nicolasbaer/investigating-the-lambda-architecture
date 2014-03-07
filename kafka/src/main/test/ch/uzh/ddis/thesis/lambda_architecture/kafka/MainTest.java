package ch.uzh.ddis.thesis.lambda_architecture.kafka;

import junit.framework.TestCase;
import org.junit.Test;

import java.io.UnsupportedEncodingException;

/**
 * @author Nicolas Baer <nicolas.baer@gmail.com>
 */
public class MainTest extends TestCase {

    @Test
    public void testStringSize() {
        String str = "1000000000000,1377986401,68.451,0,11,0,0";
        try{
            final byte[] utf8Bytes = str.getBytes("UTF-8");
            System.out.println(utf8Bytes.length);
        } catch(UnsupportedEncodingException ex){
            ex.printStackTrace();
        }
    }

}
