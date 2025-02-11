classdef FluxQueryBuilder < handle

     % BUILDFLUXQUERY Construct a parameterized Flux query for InfluxDB.
    %
    % Inputs:
    %   - bucket (string): The bucket name to query.
    %   - startTime (datetime or string): Start time for the query range.
    %   - stopTime (datetime or string): Stop time for the query range.
    %   - varargin: Name-value pairs for additional options:
    %       'Measurement' (string): Filter by measurement name.
    %       'Fields' (cell array of strings): Filter by field names.
    %       'Tags' (struct): Filter by tags (key-value pairs).
    %       'GroupBy' (cell array of strings): Group results by specified columns.
    %       'AggregateFn' (string): Apply an aggregation function (e.g., 'mean').
    %       'Window' (string): Specify a windowing period (e.g., '1h').
    %
    % Output:
    %   - fluxQuery (string): The constructed Flux query.
    
    
    properties(Access = private)
        InfluxDB = []
        Database = []
        RangeStart = []
        RangeStop = []
        Measurements = {}
        Fields = {}
        Tags = struct()
        GroupByColumns = {}
        AggregateFunction = ''
        Window = ''
        Format = 'yyyy-MM-dd''T''HH:mm:ss''Z''';
       
    end
    
    methods
        %Constructor
        function obj = FluxQueryBuilder(bucket)
            obj.Database = bucket;
        end

        % Set the client instance used for execution
        function obj = influxdb(obj, influxdb)
            obj.InfluxDB = influxdb;
        end

        % Configure time range
        function obj = range(obj, varargin)
            if nargin > 2
                start = datetime(varargin{1}, 'TimeZone','local');
                start.TimeZone = "UTC";
                stop = datetime(varargin{2}, 'TimeZone', 'local');
                stop.TimeZone = "UTC";
                obj.RangeStart = start;
                obj.RangeStop = stop;
            elseif nargin > 1
                start = datetime(varargin{1}, 'TimeZone','local');
                start.TimeZone = "UTC";
                obj.RangeStart = start;
            end
        end

        % Add measurements
        function obj = measurements(obj, varargin)
            obj.Measurements = [obj.Measurements, varargin];
        end
        
        % Add fields
        function obj = fields(obj, varargin)
            obj.Fields = [obj.Fields, varargin];
        end
        
        % Add tags
        function obj = tags(obj, tagStruct)
            fields = fieldnames(tagStruct);
            for i = 1:length(fields)
                obj.Tags.(fields{i}) = tagStruct.(fields{i});
            end
        end
        
        % Add group-by columns
        function obj = groupBy(obj, varargin)
            obj.GroupByColumns = [obj.GroupByColumns, varargin];
        end
        
        % Set aggregation function
        function obj = aggregate(obj, varargin)
            obj.Window = varargin{1};
            obj.AggregateFunction = varargin{2};
        end

        function query = build(obj, varargin)

            % Validate inputs and Configure the database
            assert(~isempty(obj.Database), 'Bucket not defined');
            if ~isstring(obj.Database) && ~ischar(obj.Database)
                error('Bucket must be a string or character vector.');
            end

            % Initialize the base query
            query = ['from(bucket: "' obj.Database '")'];

            if ~isempty(obj.RangeStart)
                obj.RangeStart = datestr(obj.RangeStart, 'yyyy-mm-ddTHH:MM:SSZ');
                query = [query ' |> range(start: ' obj.RangeStart];
                if ~isempty(obj.RangeStop)
                    obj.RangeStop = datestr(obj.RangeStop, 'yyyy-mm-ddTHH:MM:SSZ');
                    query = [query ', stop: ' obj.RangeStop];
                end
            query = [query ')'];
            end

            % Append measurements
            if ~isempty(obj.Measurements)
                measurementsFilter = obj.buildFilter(obj.Measurements, '_measurement');
                query = sprintf('%s |> filter(fn: (r) => %s)', query, measurementsFilter);
            end
            
            % Append fields
            if ~isempty(obj.Fields)
                fieldsFilter = obj.buildFilter(obj.Fields, '_field');
                query = sprintf('%s |> filter(fn: (r) => %s)', query, fieldsFilter);
            end
            
            % Append tags
            if ~isempty(fieldnames(obj.Tags))
                tagsFilter = strjoin(cellfun(@(k) sprintf('r.%s == "%s"', k, obj.Tags.(k)), ...
                    fieldnames(obj.Tags), 'UniformOutput', false), ' and ');
                query = sprintf('%s |> filter(fn: (r) => %s)', query, tagsFilter);
            end
            
            % Append group-by columns
            if ~isempty(obj.GroupByColumns)
                groupCols = strjoin(obj.GroupByColumns, '", "');
                query = sprintf('%s |> group(columns: ["%s"])', query, groupCols);
            end
            
            % Append windowing & aggregation
            if ~isempty(obj.Window) & ~isempty(obj.AggregateFunction)
                query = sprintf('%s |> aggregateWindow(every: %s, fn: %s)', query, string(obj.Window), string(obj.AggregateFunction));
            end
        end

        % Execute the fluxQuery and unpack the response
        function [result, query] = execute(obj)
            assert(~isempty(obj.InfluxDB), 'execute:clientNotSet', ...
                'the influxdb client is not set for this builder');
            query = obj.build();
            result = obj.InfluxDB.runQueryFlux(query);
        end

    end

    methods (Access = private)
            % Helper function to build filters
        function filterStr = buildFilter(~, items, fieldName)
            itemStrings = cellfun(@(x) sprintf('r.%s == "%s"', fieldName, x), items, 'UniformOutput', false);
            filterStr = strjoin(itemStrings, ' or ');
        end
    end
end





