raw = LOAD '$INPUT' USING PigStorage(',') AS (timestamp:long, station:chararray, measurement:chararray, value:chararray, unit:chararray, observation:chararray);
wind = FILTER raw BY (observation == 'SnowfallObservation') OR (measur);
wind_station = group wind by station;
average = foreach wind_station generate wind.station, avg(wind.value);
res = filter average by $1 > 74
store res into '$OUTPUT' using PigStorage(',');