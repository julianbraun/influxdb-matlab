% Initialize InfluxDB Instance 
    clear; clc;
    % Configure database
    URL = 'http://localhost:8086';
    TOKEN = 'yjcM3kyeZamfFMQTSoRaeEbvvQThziga3lyVt4fFgEAg6lgmuVqiTZHNChSLcpTs8fFz47Mvio2nQmMR7w05Xg==';
    DATABASE = 'FarmData';
    ORG = 'Ines';
    influxdb = InfluxDBF(URL, TOKEN, ORG, DATABASE);
    
    % Check the status of the InfluxDB instance
    [ok, ping] = influxdb.ping();

% Write all csv data to InfluxDB from provided path including subfoldders

    influxdb.explorer('FilePath');

% Query with Flux

    start = '06.09.2024 08:25:00';
    stop = '06.09.2024 09:20:00';
    result = influxdb.fluxQuery()...
        .range(start,stop)...
        .measurements('airSensors')...
        .fields()...
        .tags(struct())...
        .groupBy() ...
        .aggregate()...
        .execute();