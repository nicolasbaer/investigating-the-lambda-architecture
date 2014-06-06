package ch.uzh.ddis.thesis.lambda_architecture.data.esper;

import ch.uzh.ddis.thesis.lambda_architecture.data.SRBench.SRBenchDataEntry;
import com.espertech.esper.client.*;
import com.espertech.esper.client.time.CurrentTimeEvent;
import com.google.common.io.Resources;
import org.javatuples.Pair;
import org.junit.Assert;
import org.junit.Test;

import java.io.*;
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

        long lastTimestamp = 0;
        BufferedReader reader = new BufferedReader(new FileReader(csv));
        String line = null;
        ArrayList<String> res = new ArrayList<>();
        while((line = reader.readLine()) != null){
            SRBenchDataEntry entry = new SRBenchDataEntry(line);
            lastTimestamp = entry.getTimestamp();
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
                            .toString();
                    res.add(result);
                }
            }
        }

        esper.sendEvent(new CurrentTimeEvent(lastTimestamp + (1000 * 60 * 60 * 60)));

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
                        .toString();
                res.add(result);
            }
        }

        String resultFilePath = getClass().getClassLoader().getResource("srbench-results/q1").getPath();
        BufferedReader reader2 = new BufferedReader(new FileReader(resultFilePath));
        int lineCounter = 0;
        String resultLine;
        while((resultLine = reader2.readLine()) != null){
            Assert.assertEquals(resultLine, res.get(lineCounter));

            lineCounter++;
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

        long lastTimestamp = 0;
        BufferedReader reader = new BufferedReader(new FileReader(csv));
        String line;
        while((line = reader.readLine()) != null){
            SRBenchDataEntry entry = new SRBenchDataEntry(line);
            lastTimestamp = entry.getTimestamp();
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

                    results.add(result);
                }
            }
        }

        esper.sendEvent(new CurrentTimeEvent(lastTimestamp + (1000 * 60 * 60 * 60)));

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
                        .toString();
                results.add(result);
            }
        }

        String resultFilePath = getClass().getClassLoader().getResource("srbench-results/q2").getPath();
        BufferedReader reader2 = new BufferedReader(new FileReader(resultFilePath));
        int lineCounter = 0;
        String resultLine;
        while((resultLine = reader2.readLine()) != null){
            Assert.assertEquals(resultLine, results.get(lineCounter));

            lineCounter++;
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

        String csvPath = getClass().getClassLoader().getResource("srbench-testdata/q3").getPath();
        File csv = new File(csvPath);

        final ArrayList<String> results = new ArrayList<>();

        long lastTimestamp = 0;
        BufferedReader reader = new BufferedReader(new FileReader(csv));
        String line = null;
        while((line = reader.readLine()) != null){
            SRBenchDataEntry entry = new SRBenchDataEntry(line);
            lastTimestamp = entry.getTimestamp();
            esper.sendEvent(new CurrentTimeEvent(entry.getTimestamp()));
            esper.sendEvent(entry.getMap(), entry.getMeasurement());

            if(updateListener.hasNewData()){
                Pair<EventBean[], EventBean[]> eventDataTouple = updateListener.getNewData();
                EventBean[] newEvents = eventDataTouple.getValue0();

                for(int i = 0; i < newEvents.length; i++){
                    String station = (String) newEvents[i].get("station");

                    results.add(station);
                }
            }
        }

        esper.sendEvent(new CurrentTimeEvent(lastTimestamp + (1000 * 60 * 60 * 60)));

        if(updateListener.hasNewData()){
            Pair<EventBean[], EventBean[]> eventDataTouple = updateListener.getNewData();
            EventBean[] newEvents = eventDataTouple.getValue0();

            for(int i = 0; i < newEvents.length; i++){
                String station = (String) newEvents[i].get("station");

                results.add(station);
            }
        }

        Assert.assertEquals(results.get(0), "C4431");
        Assert.assertEquals(results.get(1), "C4431");
        Assert.assertTrue(results.size() == 2);

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

        String csvPath = getClass().getClassLoader().getResource("srbench-testdata/q4").getPath();
        File csv = new File(csvPath);

        final ArrayList<String> results = new ArrayList<>();

        BufferedReader reader = new BufferedReader(new FileReader(csv));
        String line = null;
        long lastTimestamp = 0;
        while((line = reader.readLine()) != null){
            SRBenchDataEntry entry = new SRBenchDataEntry(line);
            lastTimestamp = entry.getTimestamp();
            esper.sendEvent(new CurrentTimeEvent(entry.getTimestamp()));
            esper.sendEvent(entry.getMap(), entry.getMeasurement());

            if(updateListener.hasNewData()){
                Pair<EventBean[], EventBean[]> eventDataTouple = updateListener.getNewData();
                EventBean[] newEvents = eventDataTouple.getValue0();

                for(int i = 0; i < newEvents.length; i++){
                    String station = (String) newEvents[i].get("stat");
                    Double valueWind = (Double) newEvents[i].get("speed");
                    Double valueTemp = (Double) newEvents[i].get("temperature");

                    String result = new StringBuilder()
                            .append(station)
                            .append(",")
                            .append(valueWind)
                            .append(",")
                            .append(valueTemp)
                            .toString();

                    results.add(result);
                }
            }
        }

        esper.sendEvent(new CurrentTimeEvent(lastTimestamp + (1000 * 60 * 60 * 60)));

        if(updateListener.hasNewData()){
            Pair<EventBean[], EventBean[]> eventDataTouple = updateListener.getNewData();
            EventBean[] newEvents = eventDataTouple.getValue0();

            for(int i = 0; i < newEvents.length; i++){
                String station = (String) newEvents[i].get("stat");
                Double valueWind = (Double) newEvents[i].get("speed");
                Double valueTemp = (Double) newEvents[i].get("temperature");

                String result = new StringBuilder()
                        .append(station)
                        .append(",")
                        .append(valueWind)
                        .append(",")
                        .append(valueTemp)
                        .toString();

                results.add(result);
            }
        }

        Assert.assertEquals(results.get(0), "GCAP4,25.0,34.0");
        Assert.assertEquals(results.get(1), "C4431,23.0,33.0");
        Assert.assertEquals(results.get(2), "CVAV3,30.0,82.0");
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

        String csvPath = getClass().getClassLoader().getResource("srbench-testdata/q5").getPath();
        File csv = new File(csvPath);

        final ArrayList<String> results = new ArrayList<>();

        BufferedReader reader = new BufferedReader(new FileReader(csv));
        String line = null;
        long lastTimestamp = 0;
        while((line = reader.readLine()) != null){
            SRBenchDataEntry entry = new SRBenchDataEntry(line);
            lastTimestamp = entry.getTimestamp();
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
                            .toString();

                    results.add(result);
                }
            }
        }

        esper.sendEvent(new CurrentTimeEvent(lastTimestamp + (1000 * 60 * 60 * 60)));

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
                        .toString();

                results.add(result);
            }
        }

        Assert.assertEquals(results.get(0), "PABR,43.0,-9.4");
        Assert.assertEquals(results.get(1), "PABR,48.0,10.0");
    }



    @Test
    public void testSRBenchQ6() throws IOException {
        URL queryUrlRainfall = EsperFactory.class.getResource("/esper-queries/srbench-q6-rainfall.esper");
        URL queryUrlSnowfall = EsperFactory.class.getResource("/esper-queries/srbench-q6-snowfall.esper");
        URL queryUrlVisibility = EsperFactory.class.getResource("/esper-queries/srbench-q6-visibility.esper");
        String queryRainfall = Resources.toString(queryUrlRainfall, StandardCharsets.UTF_8);
        String querySnowfall = Resources.toString(queryUrlSnowfall, StandardCharsets.UTF_8);
        String queryVisibility = Resources.toString(queryUrlVisibility, StandardCharsets.UTF_8);

        EPServiceProvider eps = EsperFactory.makeEsperServiceProviderSRBench("test");
        EPAdministrator cepAdm = eps.getEPAdministrator();
        EPStatement cepStatementRainfall = cepAdm.createEPL(queryRainfall);
        EPStatement cepStatementSnowfall = cepAdm.createEPL(querySnowfall);
        EPStatement cepStatementVisibility = cepAdm.createEPL(queryVisibility);
        EsperUpdateListener updateListenerRainfall = new EsperUpdateListener();
        EsperUpdateListener updateListenerSnowfall = new EsperUpdateListener();
        EsperUpdateListener updateListenerVisibility = new EsperUpdateListener();
        cepStatementRainfall.addListener(updateListenerRainfall);
        cepStatementSnowfall.addListener(updateListenerSnowfall);
        cepStatementVisibility.addListener(updateListenerVisibility);
        EPRuntime esper = eps.getEPRuntime();

        String csvPath = getClass().getClassLoader().getResource("srbench-testdata/q6").getPath();
        File csv = new File(csvPath);

        final ArrayList<String> results = new ArrayList<>();

        BufferedReader reader = new BufferedReader(new FileReader(csv));
        String line = null;
        long lastTimestamp = 0;
        while((line = reader.readLine()) != null){
            SRBenchDataEntry entry = new SRBenchDataEntry(line);
            lastTimestamp = entry.getTimestamp();
            esper.sendEvent(new CurrentTimeEvent(entry.getTimestamp()));
            esper.sendEvent(entry.getMap(), entry.getMeasurement());

            if(updateListenerRainfall.hasNewData()){
                Pair<EventBean[], EventBean[]> eventDataTouple = updateListenerRainfall.getNewData();
                EventBean[] newEvents = eventDataTouple.getValue0();

                for(int i = 0; i < newEvents.length; i++){
                    String station = (String) newEvents[i].get("station");

                    results.add(station);
                }
            }
            if(updateListenerSnowfall.hasNewData()){
                Pair<EventBean[], EventBean[]> eventDataTouple = updateListenerSnowfall.getNewData();
                EventBean[] newEvents = eventDataTouple.getValue0();

                for(int i = 0; i < newEvents.length; i++){
                    String station = (String) newEvents[i].get("station");

                    results.add(station);
                }
            }
            if(updateListenerVisibility.hasNewData()){
                Pair<EventBean[], EventBean[]> eventDataTouple = updateListenerVisibility.getNewData();
                EventBean[] newEvents = eventDataTouple.getValue0();

                for(int i = 0; i < newEvents.length; i++){
                    String station = (String) newEvents[i].get("station");

                    results.add(station);
                }
            }
        }

        esper.sendEvent(new CurrentTimeEvent(lastTimestamp + (1000 * 60 * 60 * 60)));

        if(updateListenerRainfall.hasNewData()){
            Pair<EventBean[], EventBean[]> eventDataTouple = updateListenerRainfall.getNewData();
            EventBean[] newEvents = eventDataTouple.getValue0();

            for(int i = 0; i < newEvents.length; i++){
                String station = (String) newEvents[i].get("station");

                results.add(station);
            }
        }
        if(updateListenerSnowfall.hasNewData()){
            Pair<EventBean[], EventBean[]> eventDataTouple = updateListenerSnowfall.getNewData();
            EventBean[] newEvents = eventDataTouple.getValue0();

            for(int i = 0; i < newEvents.length; i++){
                String station = (String) newEvents[i].get("station");

                results.add(station);
            }
        }
        if(updateListenerVisibility.hasNewData()){
            Pair<EventBean[], EventBean[]> eventDataTouple = updateListenerVisibility.getNewData();
            EventBean[] newEvents = eventDataTouple.getValue0();

            for(int i = 0; i < newEvents.length; i++){
                String station = (String) newEvents[i].get("station");

                results.add(station);
            }
        }

        Assert.assertEquals(results.get(0), "MTHN2");
        Assert.assertEquals(results.get(1), "PANT");
        Assert.assertEquals(results.get(2), "KMMV");
        Assert.assertTrue(results.size() == 3);
    }



    @Test
    public void testSRBenchQ7() throws IOException {
        URL queryUrl = EsperFactory.class.getResource("/esper-queries/srbench-q7.esper");
        String query = Resources.toString(queryUrl, StandardCharsets.UTF_8);

        EPServiceProvider eps = EsperFactory.makeEsperServiceProviderSRBench("test");
        EPAdministrator cepAdm = eps.getEPAdministrator();
        EPStatement cepStatement = cepAdm.createEPL(query);
        EsperUpdateListener updateListener = new EsperUpdateListener();
        cepStatement.addListener(updateListener);
        EPRuntime esper = eps.getEPRuntime();

        String csvPath = getClass().getClassLoader().getResource("srbench-testdata/q7").getPath();
        File csv = new File(csvPath);

        final ArrayList<String> results = new ArrayList<>();

        BufferedReader reader = new BufferedReader(new FileReader(csv));
        String line = null;
        long lastTimestamp = 0;
        while((line = reader.readLine()) != null){
            SRBenchDataEntry entry = new SRBenchDataEntry(line);
            lastTimestamp = entry.getTimestamp();
            esper.sendEvent(new CurrentTimeEvent(entry.getTimestamp()));
            esper.sendEvent(entry.getMap(), entry.getMeasurement());

            if(updateListener.hasNewData()){
                Pair<EventBean[], EventBean[]> eventDataTouple = updateListener.getNewData();
                EventBean[] newEvents = eventDataTouple.getValue0();

                for(int i = 0; i < newEvents.length; i++){
                    String station = (String) newEvents[i].get("stat");

                    results.add(station);
                }
            }
        }

        esper.sendEvent(new CurrentTimeEvent(lastTimestamp + (1000 * 60 * 60 * 60)));

        if(updateListener.hasNewData()){
            Pair<EventBean[], EventBean[]> eventDataTouple = updateListener.getNewData();
            EventBean[] newEvents = eventDataTouple.getValue0();

            for(int i = 0; i < newEvents.length; i++){
                String station = (String) newEvents[i].get("stat");

                results.add(station);
            }
        }

        Assert.assertEquals(results.get(0), "ABC");
        Assert.assertEquals(results.get(1), "KMMV");
        Assert.assertEquals(results.get(2), "KMMV");
        Assert.assertEquals(results.size(), 3);
    }
}
