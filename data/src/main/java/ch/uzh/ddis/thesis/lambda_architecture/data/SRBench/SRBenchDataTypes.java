package ch.uzh.ddis.thesis.lambda_architecture.data.SRBench;

/**
 * Defines the types available for the srbench dataset.
 *
 * @author Nicolas Baer <nicolas.baer@gmail.com>
 */
public class SRBenchDataTypes{

    public static final String srBench = "srBench";

    public enum Observation {
        PrecipitationObservation,
        PressureObservation,
        RainfallObservation,
        RelativeHumidityObservation,
        SnowfallObservation,
        TemperatureObservation,
        VisibilityObservation,
        WindDirectionObservation,
        WindObservation,
        WindSpeedObservation
    }


    public enum Measurement {
        AirTemperature,
        DewPoint,
        PeakWindDirection,
        PeakWindSpeed,
        Pressure,
        RelativeHumidity,
        SoilTemperature,
        Visibility,
        WaterTemperature,
        WindDirection,
        WindGust,
        WindSpeed,
        Precipitation,
        PrecipitationAccumulated,
        PrecipitationSmoothed,
        SnowDepth,
        SnowInterval,
        SnowSmoothed,
        SoilMoisture,
        SoilMoistureTension
    }



}
