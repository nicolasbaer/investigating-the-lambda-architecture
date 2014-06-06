package ch.uzh.ddis.thesis.lambda_architecture.data.esper;

import ch.uzh.ddis.thesis.lambda_architecture.data.SRBench.SRBenchDataTypes;
import ch.uzh.ddis.thesis.lambda_architecture.data.debs.DebsDataTypes;
import com.espertech.esper.client.EPAdministrator;
import com.espertech.esper.client.EPServiceProvider;
import org.junit.Assert;
import org.junit.Test;

public class EsperFactoryTest {

    @Test
    public void testMakeEsperServiceProviderSRBench() throws Exception {

        EPServiceProvider cep = EsperFactory.makeEsperServiceProviderSRBench("test");
        EPAdministrator epAdm = cep.getEPAdministrator();

        Assert.assertNotNull(epAdm.getConfiguration().getEventType(SRBenchDataTypes.srBench));

        Assert.assertNotNull(epAdm.getConfiguration().getEventType(SRBenchDataTypes.Observation.PrecipitationObservation.name()));
        Assert.assertNotNull(epAdm.getConfiguration().getEventType(SRBenchDataTypes.Observation.PressureObservation.name()));
        Assert.assertNotNull(epAdm.getConfiguration().getEventType(SRBenchDataTypes.Observation.RainfallObservation.name()));
        Assert.assertNotNull(epAdm.getConfiguration().getEventType(SRBenchDataTypes.Observation.RelativeHumidityObservation.name()));
        Assert.assertNotNull(epAdm.getConfiguration().getEventType(SRBenchDataTypes.Observation.SnowfallObservation.name()));
        Assert.assertNotNull(epAdm.getConfiguration().getEventType(SRBenchDataTypes.Observation.TemperatureObservation.name()));
        Assert.assertNotNull(epAdm.getConfiguration().getEventType(SRBenchDataTypes.Observation.WindSpeedObservation.name()));


        Assert.assertNotNull(epAdm.getConfiguration().getEventType(SRBenchDataTypes.Measurement.AirTemperature.name()));
        Assert.assertNotNull(epAdm.getConfiguration().getEventType(SRBenchDataTypes.Measurement.DewPoint.name()));
        Assert.assertNotNull(epAdm.getConfiguration().getEventType(SRBenchDataTypes.Measurement.PeakWindDirection.name()));
        Assert.assertNotNull(epAdm.getConfiguration().getEventType(SRBenchDataTypes.Measurement.PeakWindSpeed.name()));
        Assert.assertNotNull(epAdm.getConfiguration().getEventType(SRBenchDataTypes.Measurement.Pressure.name()));
        Assert.assertNotNull(epAdm.getConfiguration().getEventType(SRBenchDataTypes.Measurement.RelativeHumidity.name()));
        Assert.assertNotNull(epAdm.getConfiguration().getEventType(SRBenchDataTypes.Measurement.SoilTemperature.name()));
        Assert.assertNotNull(epAdm.getConfiguration().getEventType(SRBenchDataTypes.Measurement.Visibility.name()));
        Assert.assertNotNull(epAdm.getConfiguration().getEventType(SRBenchDataTypes.Measurement.WaterTemperature.name()));
        Assert.assertNotNull(epAdm.getConfiguration().getEventType(SRBenchDataTypes.Measurement.WindDirection.name()));
        Assert.assertNotNull(epAdm.getConfiguration().getEventType(SRBenchDataTypes.Measurement.WindGust.name()));
        Assert.assertNotNull(epAdm.getConfiguration().getEventType(SRBenchDataTypes.Measurement.WindSpeed.name()));
        Assert.assertNotNull(epAdm.getConfiguration().getEventType(SRBenchDataTypes.Measurement.Precipitation.name()));
        Assert.assertNotNull(epAdm.getConfiguration().getEventType(SRBenchDataTypes.Measurement.PrecipitationAccumulated.name()));
        Assert.assertNotNull(epAdm.getConfiguration().getEventType(SRBenchDataTypes.Measurement.PrecipitationSmoothed.name()));
        Assert.assertNotNull(epAdm.getConfiguration().getEventType(SRBenchDataTypes.Measurement.SnowDepth.name()));
        Assert.assertNotNull(epAdm.getConfiguration().getEventType(SRBenchDataTypes.Measurement.SnowInterval.name()));
        Assert.assertNotNull(epAdm.getConfiguration().getEventType(SRBenchDataTypes.Measurement.SnowSmoothed.name()));
        Assert.assertNotNull(epAdm.getConfiguration().getEventType(SRBenchDataTypes.Measurement.SoilMoisture.name()));
        Assert.assertNotNull(epAdm.getConfiguration().getEventType(SRBenchDataTypes.Measurement.SoilMoistureTension.name()));

    }


    @Test
    public void testMakeEsperServiceProviderDebs() throws Exception {

        EPServiceProvider cep = EsperFactory.makeEsperServiceProviderDebs("test");
        EPAdministrator epAdm = cep.getEPAdministrator();

        Assert.assertNotNull(epAdm.getConfiguration().getEventType(DebsDataTypes.Debs));
        Assert.assertNotNull(epAdm.getConfiguration().getEventType(DebsDataTypes.Measurement.Load.name()));
        Assert.assertNotNull(epAdm.getConfiguration().getEventType(DebsDataTypes.Measurement.Work.name()));



    }
}