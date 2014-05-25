raw = LOAD '$INPUT' USING PigStorage(',') AS (timestamp:long, station:chararray, measurement:chararray, value:chararray, unit:chararray, observation:chararray);
wind = FILTER raw BY measurement == 'WindSpeed';
wind_station = group wind by station;
res = foreach wind_station generate wind.station, avg(wind.value);
store res into '$OUTPUT' using PigStorage(',');