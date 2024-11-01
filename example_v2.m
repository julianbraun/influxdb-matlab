clear; clc;
% addpath('influxdb-client');
tic

% Configure database
URL = 'http://localhost:8086';
TOKEN = 'aPuxv5Ma45PyCSDqu2I2sEpCaYC8_jJ85yFF8deDcfyl0ce9n3RA6ZYccYCIF1Ya8P9TWHT9TiXbHQXyc9n9Tw==';
DATABASE = 'test';
ORG = 'ines';
influxdb = InfluxDBv2(URL, TOKEN, ORG, DATABASE);

% Check the status of the InfluxDB instance
[ok, ping] = influxdb.ping();

% Change the current database
influxdb.use('test');

% Show databases
influxdb.databases()

% Write data
series1 = Series('position') ...
    .tags('city', 'antwerp', 'country', 'belgium') ...
    .fields('x', rand(86400,1)*825, 'y', rand(86400,1)*433.65) ...
    .time(datetime('now', 'TimeZone', 'local', 'Format', 'yyy-MM-dd HH:mm:ss.SSS')-seconds(rand(86400,1)*1000000));

influxdb.writer().append(series1).execute()
toc
tic
% Read data
result = influxdb.query('position').execute();
result.series('position').timetable('Europe/Berlin')
toc