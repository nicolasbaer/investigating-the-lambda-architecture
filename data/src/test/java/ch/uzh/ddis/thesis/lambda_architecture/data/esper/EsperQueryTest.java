package ch.uzh.ddis.thesis.lambda_architecture.data.esper;

import ch.uzh.ddis.thesis.lambda_architecture.data.SRBench.SRBenchDataEntry;
import com.espertech.esper.client.*;
import com.espertech.esper.client.time.CurrentTimeEvent;
import com.google.common.io.Resources;
import org.javatuples.Pair;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;

/**
 * @author Nicolas Baer <nicolas.baer@gmail.com>
 */
public class EsperQueryTest {

    @Test
    public void testSRBenchQ1() throws IOException {
        URL queryUrl = EsperFactory.class.getResource("/esper-queries/srbench-q1.esper");
        String query = Resources.toString(queryUrl, StandardCharsets.UTF_8);

        EPServiceProvider eps = EsperFactory.makeEsperServiceProviderSRBench("test-q1");
        EPAdministrator cepAdm = eps.getEPAdministrator();
        EPStatement cepStatement = cepAdm.createEPL(query);
        EsperUpdateListener updateListener = new EsperUpdateListener();
        cepStatement.addListener(updateListener);
        EPRuntime esper = eps.getEPRuntime();

        String csvPath = getClass().getClassLoader().getResource("bill_sample.csv").getPath();
        File csv = new File(csvPath);

        final ArrayList<String> results = new ArrayList<>();

        BufferedReader reader = new BufferedReader(new FileReader(csv));
        String line = null;
        while((line = reader.readLine()) != null){
            SRBenchDataEntry entry = new SRBenchDataEntry(line);
            esper.sendEvent(new CurrentTimeEvent(entry.getTimestamp()));
            esper.sendEvent(entry.getMap(), entry.getMeasurement());

            System.out.println(line);

            if(updateListener.hasNewData()){
                Pair<EventBean[], EventBean[]> eventDataTouple = updateListener.getNewData();
                EventBean[] newEvents = eventDataTouple.getValue0();
                System.out.println("results");
                System.out.println("..........................................");
                for(int i = 0; i < newEvents.length; i++){
                    String station = (String) newEvents[i].get("station");
                    String value = String.valueOf(newEvents[i].get("value"));
                    String unit = (String) newEvents[i].get("unit");

                    String result = new StringBuilder()
                            .append(station)
                            .append(",")
                            .append(value)
                            .append(",")
                            .append(unit)
                            .append(",")
                            .toString();

                    System.out.println(result);
                }
                System.out.println(entry.getTimestamp());
                System.out.println("----------------------------------------------");
            }
        }
    }


    @Test
    public void testSRBenchQ2() throws IOException {
        URL queryUrl = EsperFactory.class.getResource("/esper-queries/srbench-q2.esper");
        String query = Resources.toString(queryUrl, StandardCharsets.UTF_8);

        EPServiceProvider eps = EsperFactory.makeEsperServiceProviderSRBench("test-q1");
        EPAdministrator cepAdm = eps.getEPAdministrator();
        EPStatement cepStatement = cepAdm.createEPL(query);
        EsperUpdateListener updateListener = new EsperUpdateListener();
        cepStatement.addListener(updateListener);
        EPRuntime esper = eps.getEPRuntime();

        String csvPath = getClass().getClassLoader().getResource("bill_sample.csv").getPath();
        File csv = new File(csvPath);

        final ArrayList<String> results = new ArrayList<>();

        BufferedReader reader = new BufferedReader(new FileReader(csv));
        String line = null;
        while((line = reader.readLine()) != null){
            SRBenchDataEntry entry = new SRBenchDataEntry(line);
            esper.sendEvent(new CurrentTimeEvent(entry.getTimestamp()));
            esper.sendEvent(entry.getMap(), entry.getMeasurement());

            if(updateListener.hasNewData()){
                Pair<EventBean[], EventBean[]> eventDataTouple = updateListener.getNewData();
                EventBean[] newEvents = eventDataTouple.getValue0();

                for(int i = 0; i < newEvents.length; i++){
                    String station = (String) newEvents[i].get("station");
                    String value = String.valueOf(newEvents[i].get("value"));
                    String unit = (String) newEvents[i].get("unit");

                    String result = new StringBuilder()
                            .append(station)
                            .append(",")
                            .append(value)
                            .append(",")
                            .append(unit)
                            .append(",")
                            .toString();

                    System.out.println(result);
                }
                System.out.println(entry.getTimestamp());
                System.out.println("----------------------------------------------");
            }
        }
    }


    @Test
    public void testSRBenchQ3() throws IOException {
        URL queryUrl = EsperFactory.class.getResource("/esper-queries/srbench-q3.esper");
        String query = Resources.toString(queryUrl, StandardCharsets.UTF_8);

        EPServiceProvider eps = EsperFactory.makeEsperServiceProviderSRBench("test-q1");
        EPAdministrator cepAdm = eps.getEPAdministrator();
        EPStatement cepStatement = cepAdm.createEPL(query);
        EsperUpdateListener updateListener = new EsperUpdateListener();
        cepStatement.addListener(updateListener);
        EPRuntime esper = eps.getEPRuntime();

        String csvPath = getClass().getClassLoader().getResource("bill_sample.csv").getPath();
        File csv = new File(csvPath);

        final ArrayList<String> results = new ArrayList<>();

        BufferedReader reader = new BufferedReader(new FileReader(csv));
        String line = null;
        while((line = reader.readLine()) != null){
            SRBenchDataEntry entry = new SRBenchDataEntry(line);
            esper.sendEvent(new CurrentTimeEvent(entry.getTimestamp()));
            esper.sendEvent(entry.getMap(), entry.getMeasurement());

            if(updateListener.hasNewData()){
                Pair<EventBean[], EventBean[]> eventDataTouple = updateListener.getNewData();
                EventBean[] newEvents = eventDataTouple.getValue0();

                for(int i = 0; i < newEvents.length; i++){
                    String station = (String) newEvents[i].get("station");
                    Double value = (Double) newEvents[i].get("speed");

                    String result = new StringBuilder()
                            .append(station)
                            .append(",")
                            .append(value)
                            .append(",")
                            .toString();

                    System.out.println(result);
                }
                System.out.println(entry.getTimestamp());
                System.out.println("----------------------------------------------");
            }
        }
    }


    @Test
    public void testSRBenchQ4() throws IOException {
        URL queryUrl = EsperFactory.class.getResource("/esper-queries/srbench-q4.esper");
        String query = Resources.toString(queryUrl, StandardCharsets.UTF_8);

        EPServiceProvider eps = EsperFactory.makeEsperServiceProviderSRBench("test");
        EPAdministrator cepAdm = eps.getEPAdministrator();
        EPStatement cepStatement = cepAdm.createEPL(query);
        EsperUpdateListener updateListener = new EsperUpdateListener();
        cepStatement.addListener(updateListener);
        EPRuntime esper = eps.getEPRuntime();

        String csvPath = getClass().getClassLoader().getResource("bill_sample.csv").getPath();
        File csv = new File(csvPath);

        final ArrayList<String> results = new ArrayList<>();

        BufferedReader reader = new BufferedReader(new FileReader(csv));
        String line = null;
        while((line = reader.readLine()) != null){
            SRBenchDataEntry entry = new SRBenchDataEntry(line);
            esper.sendEvent(new CurrentTimeEvent(entry.getTimestamp()));
            esper.sendEvent(entry.getMap(), entry.getMeasurement());

            if(updateListener.hasNewData()){
                Pair<EventBean[], EventBean[]> eventDataTouple = updateListener.getNewData();
                EventBean[] newEvents = eventDataTouple.getValue0();

                for(int i = 0; i < newEvents.length; i++){
                    String station = (String) newEvents[i].get("stat");
                    Double value = (Double) newEvents[i].get("speed");

                    String result = new StringBuilder()
                            .append(station)
                            .append(",")
                            .append(value)
                            .append(",")
                            .toString();

                    System.out.println(result);
                }
                System.out.println(entry.getTimestamp());
                System.out.println("----------------------------------------------");
            }
        }
    }


    @Test
    public void testSRBenchQ5() throws IOException {
        URL queryUrl = EsperFactory.class.getResource("/esper-queries/srbench-q5.esper");
        String query = Resources.toString(queryUrl, StandardCharsets.UTF_8);

        EPServiceProvider eps = EsperFactory.makeEsperServiceProviderSRBench("test");
        EPAdministrator cepAdm = eps.getEPAdministrator();
        EPStatement cepStatement = cepAdm.createEPL(query);
        EsperUpdateListener updateListener = new EsperUpdateListener();
        cepStatement.addListener(updateListener);
        EPRuntime esper = eps.getEPRuntime();

        String csvPath = getClass().getClassLoader().getResource("nevadastorm_sample.csv").getPath();
        File csv = new File(csvPath);

        final ArrayList<String> results = new ArrayList<>();

        BufferedReader reader = new BufferedReader(new FileReader(csv));
        String line = null;
        while((line = reader.readLine()) != null){
            SRBenchDataEntry entry = new SRBenchDataEntry(line);
            esper.sendEvent(new CurrentTimeEvent(entry.getTimestamp()));
            esper.sendEvent(entry.getMap(), entry.getMeasurement());

            if(updateListener.hasNewData()){
                Pair<EventBean[], EventBean[]> eventDataTouple = updateListener.getNewData();
                EventBean[] newEvents = eventDataTouple.getValue0();

                for(int i = 0; i < newEvents.length; i++){
                    String station = (String) newEvents[i].get("stat");
                    Double wvalue = (Double) newEvents[i].get("wvalue");
                    Double avalue = (Double) newEvents[i].get("avalue");

                    String result = new StringBuilder()
                            .append(station)
                            .append(",")
                            .append(wvalue)
                            .append(",")
                            .append(avalue)
                            .append(",")
                            .toString();

                    System.out.println(result);
                }
                System.out.println(entry.getTimestamp());
                System.out.println("----------------------------------------------");
            }
        }
    }


/*
    @Test
    public void testSRBenchQ6() throws IOException {
        URL queryUrl = EsperFactory.class.getResource("/esper-queries/srbench-q6.esper");
        String query = Resources.toString(queryUrl, StandardCharsets.UTF_8);

        EPServiceProvider eps = EsperFactory.makeEsperServiceProviderSRBench("test");
        EPAdministrator cepAdm = eps.getEPAdministrator();
        EPStatement cepStatement = cepAdm.createEPL(query);
        EsperUpdateListener updateListener = new EsperUpdateListener();
        cepStatement.addListener(updateListener);
        EPRuntime esper = eps.getEPRuntime();

        String csvPath = getClass().getClassLoader().getResource("nevadastorm_sample.csv").getPath();
        File csv = new File(csvPath);

        final ArrayList<String> results = new ArrayList<>();

        BufferedReader reader = new BufferedReader(new FileReader(csv));
        String line = null;
        while((line = reader.readLine()) != null){
            SRBenchDataEntry entry = new SRBenchDataEntry(line);
            esper.sendEvent(new CurrentTimeEvent(entry.getTimestamp()));
            esper.sendEvent(entry.getMap(), entry.getMeasurement());

            if(updateListener.hasNewData()){
                Pair<EventBean[], EventBean[]> eventDataTouple = updateListener.getNewData();
                EventBean[] newEvents = eventDataTouple.getValue0();

                for(int i = 0; i < newEvents.length; i++){
                    String station = (String) newEvents[i].get("stat");

                    String result = new StringBuilder()
                            .append(station)
                            .append(",")
                            .append(",")
                            .toString();

                    System.out.println(result);
                }
                System.out.println(entry.getTimestamp());
                System.out.println("----------------------------------------------");
            }
        }
    }

*/
}
