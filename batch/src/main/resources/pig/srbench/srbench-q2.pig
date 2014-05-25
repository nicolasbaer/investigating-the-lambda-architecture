raw = LOAD '$INPUT' USING PigStorage(',') AS (timestamp:long, station:chararray, measurement:chararray, value:chararray, unit:chararray, observation:chararray);
tmp = FILTER raw BY observation == 'PrecipitationObservation';
res = DISTINCT tmp;
store res into '$OUTPUT' using PigStorage(',');