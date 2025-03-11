classdef InfluxDBF < handle
    
    properties(Access = private)
        Url = ''
        Token = ''
        Database = ''
        Organization = ''
        ReadTimeout = 10
        WriteTimeout = 10
    end
    
    methods
        % Constructor
        function obj = InfluxDBF(url, token, org, database)
            obj.Url = url;
            obj.Token = token;
            obj.Organization = org;
            obj.use(database);
            obj.Database = database;
        end
        
        % Set the read timeout
        function obj = setReadTimeout(obj, timeout)
            obj.ReadTimeout = timeout;
        end
        
        % Set the write timeout
        function obj = setWriteTimeout(obj, timeout)
            obj.WriteTimeout = timeout;
        end
        
        % Check the status of the InfluxDB instance
        function [ok, millis] = ping(obj)
            try
                timer = tic;
                webread([obj.Url '/ping']);
                millis = toc(timer) * 1000;
                ok = true;
            catch
                millis = Inf;
                ok = false;
            end
        end
        
        % Show databases
        function databases = databases(obj)
            result = obj.runCommand('SHOW DATABASES');
            databases = result.series().field('name');
        end
        
        % Change the current database
        function obj = use(obj, database)
            obj.Database = database;
            
            % Delete existing retention policies associated to the new
            % database.
            params = {['org=' obj.Organization]};
            url = [obj.Url '/api/v2/dbrps?' strjoin(params, '&')];
            opts = weboptions('KeyName', 'Authorization', 'KeyValue',['Token ' obj.Token]);
            response = webread(url, opts);
            dbrps = response.content;
            
            for i=1:length(dbrps)
                dbrp = dbrps(i);
                if strcmp(dbrp.database, obj.Database)
                    params = {['org=' obj.Organization]};
                    url = [obj.Url '/api/v2/dbrps/' dbrp.id '?' strjoin(params, '&')];
                    opts = weboptions('RequestMethod', 'delete', 'KeyName', 'Authorization', 'KeyValue',['Token ' obj.Token]);
                    webread(url, opts);
                end
            end
            
            % Get bucket ID
            params = {['org=' obj.Organization], ['name=' obj.Database]};
            url = [obj.Url '/api/v2/buckets?' strjoin(params, '&')];
            opts = weboptions('KeyName', 'Authorization', 'KeyValue',['Token ' obj.Token]);
            response = webread(url, opts);
            bucketID = response.buckets.id;
            
            % Create DBRP mapping
            data = struct;
            data.bucketID = bucketID;
            data.database = obj.Database;
            data.org = obj.Organization;
            data.retention_policy = [obj.Database '-rp'];
            url = [obj.Url '/api/v2/dbrps'];
            opts = weboptions('MediaType','application/json','KeyName','Authorization','KeyValue',['Token ' obj.Token]);
            webwrite(url, data, opts);
        end
        
        % Execute a query string
        function result = runQuery(obj, query, database, epoch)
            if nargin < 3 || isempty(database)
                database = obj.Database;
            end
            if nargin < 4 || isempty(epoch)
                epoch = 'ms';
            else
                TimeUtils.validateEpoch(epoch);
            end
            if iscell(query)
                query = strjoin(query, ';');
            end
            params = {['db=' database], ['epoch=' epoch], ['q=' query]};
            url = [obj.Url '/query?' strjoin(params, '&')];
            opts = weboptions('Timeout', obj.ReadTimeout, ...
                'KeyName', 'Authorization', 'KeyValue',['Token ' obj.Token]);
            response = webread(url, opts);
            result = QueryResult.from(response, epoch);
        end
        
        function result = runQueryFlux(obj, fluxQuery, url, org, token)
            if nargin < 3 || isempty(url)
                url = obj.Url;
            end
            if nargin < 4 || isempty(org)
                org = obj.Organization;
            end
            if nargin < 5 || isempty(token)
                token = obj.Token;
            end
            % Define the URL for the query
            endpoint = strcat(url, '/api/v2/query?org=', org);

            % Set up the options for webread (HTTP POST)
            options = weboptions('HeaderFields', {'Authorization', ['Token ', token]; 'Accept', 'application/csv'}, 'MediaType', 'application/json');

            % Prepare query as a structure
            body = struct('query', fluxQuery);

            % Send the query
            result = webwrite(endpoint, body, options);

        end

        % Obtain a query builder
        function builder = query(obj, varargin)
            if nargin > 2
                builder = QueryBuilder().series(varargin).influxdb(obj);
            elseif nargin > 1
                builder = QueryBuilder().series(varargin{1}).influxdb(obj);
            else
                builder = QueryBuilder().influxdb(obj);
            end
        end

        % Obtain a flux query builder
        function builder = fluxQuery(obj, bucket)
            % Check if the bucket is provided or default to obj.Database
            if nargin < 2 || isempty(bucket)
                assert(~isempty(obj.Database), 'Database property is not set');
                bucket = obj.Database;
            end
            builder = FluxQueryBuilder(bucket).influxdb(obj);
        end

        % Execute a write of a line protocol string
        function [] = runWrite(obj, lines, database, precision, retention, consistency)
            params = {};
            if nargin > 2 && ~isempty(database)
                params{end + 1} = ['db=' urlencode(database)];
            else
                params{end + 1} = ['db=' urlencode(obj.Database)];
            end
            if nargin > 3 && ~isempty(precision)
                TimeUtils.validatePrecision(precision);
                params{end + 1} = ['precision=' precision];
            end
            if nargin > 4  &&  ~isempty(retention)
                params{end + 1} = ['rp=' urlencode(retention)];
            end
            if nargin > 5  &&  ~isempty(consistency)
                assert(any(strcmp(consistency, {'any', 'one', 'quorum', 'all'})), ...
                    'consistency:unknown', '"%s" is not a valid consistency', consistency);
                params{end + 1} = ['consistency=' consistency];
            end
            url = [obj.Url '/write?' strjoin(params, '&')];
            opts = weboptions('Timeout', obj.WriteTimeout, ...
                'KeyName', 'Authorization', 'KeyValue',['Token ' obj.Token]);
            try
            webwrite(url, lines, opts);
            catch
                warning('problem in webwrite')
            end
        end

        % Obtain a write builder
        function builder = writer(obj)
            builder = WriteBuilder().influxdb(obj);
        end
        
        function deleteData(obj, startTime, stopTime, bucket, predicate)
                % Deletes data from a given database bucket between startTime and stopTime
                % Optionally filter with a predicate to delete specific points (based on tags/fields)
    
                % Validate inputs
                if nargin < 3 || isempty(startTime) || isempty(stopTime)
                 error('Start and stop times must be provided.');
                end
    
                if nargin < 4 || isempty(bucket)
                    bucket = obj.Database;  % Use the current database if none specified
                end
                
                if nargin < 5 || isempty(predicate)
                    predicate = '';  % Delete all data if no predicate is provided
                end

                % Convert startTime and stopTime to proper string format
                startTimeStr = datestr(startTime, 'yyyy-mm-ddTHH:MM:SSZ');  % Convert to ISO 8601/RFC3339 format
                stopTimeStr = datestr(stopTime, 'yyyy-mm-ddTHH:MM:SSZ');

                % Construct the delete URL and parameters
                params = {['org=' urlencode(obj.Organization)],['bucket=' urlencode(bucket)]};
                url = [obj.Url '/api/v2/delete?' strjoin(params, '&')];
            
                % Define the delete request body
                data = struct;
                data.start = startTimeStr;
                data.stop = stopTimeStr;
                if ~isempty(predicate)
                    data.predicate = predicate;  % Predicate for filtering (optional)
                end
           
                % Send the delete request
                opts = weboptions('RequestMethod', 'post','MediaType', 'application/json','KeyName', 'Authorization', 'KeyValue', ['Token ' obj.Token]);
                webwrite(url, data, opts);
            
                fprintf('Data deleted from %s bucket between %s and %s.\n', bucket, startTime, stopTime);
        end

        function editData(obj, startTime, stopTime, bucket, predicate, series)

            obj.deleteData(startTime, stopTime, bucket, predicate);

            obj.writer().append(series).execute();
        end

        function explorer = explorer(obj, rootDir)
            explorer = FolderExplorer().influxdb(obj);
            explorer.traverseDirectory(rootDir);

        end

        % Execute other queries or commands
        function result = runCommand(obj, command, varargin)
            idx = find(cellfun(@ischar, varargin), 1, 'first');
            database = iif(isempty(idx), '', varargin{idx});
            idx = find(cellfun(@islogical, varargin), 1, 'first');
            requiresPost = iif(isempty(idx), false, varargin{idx});          

            if isempty(database)
                params = {'q', command};
            else
                params = {'db', database, 'q', command};
            end
            url = [obj.Url '/query'];
            opts = weboptions('KeyName', 'Authorization', 'KeyValue',['Token ' obj.Token]);
            if requiresPost
                opts.Timeout = obj.WriteTimeout;
                response = webwrite(url, params{:}, opts);
            else
                opts.Timeout = obj.ReadTimeout;
                response = webread(url, params{:}, opts);
            end
            result = QueryResult.from(response);
        end
    end
    
end