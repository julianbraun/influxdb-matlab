classdef Series < handle
    
    properties(Access = private)
        Name = []
        Tags = {}
        Fields = {}
        Time = []
    end
    
    methods
        % Constructor
        function obj = Series(name)
            obj.Name = name;
        end
        
        % Add a tag
        function obj = tag(obj, key, value)
            if isnumeric(value) || islogical(value)
                obj.Tags{end + 1} = [Series.safeKey(key) '=' value];
            elseif ischar(value)
                obj.Tags{end + 1} = [Series.safeKey(key) '=' Series.safeKey(value)];
            else
                error('unsupported tag type');
            end
        end
        
        % Add multiple tags at once
        function obj = tags(obj, varargin)
            forEachPair(varargin, @(k, v) obj.tag(k, v));
        end
        
        % Add a field value
        function obj = field(obj, key, value)
            if ischar(value)
                field = struct('key', key, 'value', {{value}});
                obj.Fields{end + 1} = field;
            elseif isempty(value)
                % ignore field with empty value
            elseif isnumeric(value) || islogical(value)
                field = struct('key', key, 'value', value);
                obj.Fields{end + 1} = field;
            elseif iscell(value)
                field = struct('key', key, 'value', {value});
                obj.Fields{end + 1} = field;
            else
                error('unsupported value type');
            end
        end
        
        % Add multiple fields at once
        function obj = fields(obj, varargin)
            forEachPair(varargin, @(k, v) obj.field(k, v));
        end
        
        % Set the time
        function obj = time(obj, time)
            if isdatetime(time)
                obj.Time = time;
            else
                error('unsupported time type');
            end
        end
        
        % Import data from other structures
        function obj = import(obj, data)
            if istimetable(data) || istable(data)
                insert = @(x) obj.field(x, data.(x));
                cellfun(insert, data.Properties.VariableNames);
                if istimetable(data)
                    obj.time(data.Properties.RowTimes);
                end
            else
                error('unsupported import data type');
            end
        end
        
        % Format to Line Protocol
        function lines = toLine(obj, precision)
            time_length = length(obj.Time);
            field_lengths = unique(cellfun(@(x) length(x.value), obj.Fields));
            
            % Check if the series name is valid
            assert(~isempty(obj.Name), ...
                'toLine:emptyName', 'series name cannot be empty');
            
            % Return empty if there are no fields
            if isempty(field_lengths)
                lines = '';
                return;
            end
            
            % Make sure the dimensions match
            assert(length(field_lengths) == 1, ...
                'toLine:sizeMismatch', 'all fields must have the same length');
            assert(time_length == field_lengths || time_length == 0, ...
                'toLine:sizeMismatch', 'time and fields must have the same length');
            assert(~isempty(obj.Time) || field_lengths == 1, ...
                'toLine:emptyTime', 'the time vector cannot be empty');
            
            % Obtain the time precision scale
            if time_length > 0
                if nargin < 2, precision = 'ms'; end
                scale = TimeUtils.scaleOfPrecision(precision);
                timestamp = int64(scale * posixtime(obj.Time));
            end
            
            % Create a line for each sample
            measurement = Series.safeMeasurement(obj.Name);
            prefix = strjoin([{measurement}, obj.Tags], ',');

            % Change all values into strings and put them into a cells
            % Everything but numbers needs quotation marks
            cells = cellfun(@(x) iscell(x.value), obj.Fields);
            values = cell(size(cells));
            values(~cells) = cellfun(@(x) string(x.value), obj.Fields(~cells),'UniformOutput',false);
            values(cells) = cellfun(@(x) append("*!*", string(x.value), "*!*"), obj.Fields(cells),'UniformOutput',false);

            % Get the values into a matrix for better handling
            values = cat(2,struct('sx', values).sx);
            % Assign empty values to strings accordingly
            values(values == '*!**!*') = missing;
            % Replace unauthorized expressions
            values = Series.safeValue(values);

            % Change all keys into a string array and replace unauthorized
            % expressions
            keys = Series.safeValue(string(cellfun(@(x) x.key, obj.Fields,'UniformOutput',false)));

            % Write the keys before the according values
            lines = append(keys, "=", values);
            % Change missing values to empty strings
            lines(ismissing(lines)) = "";
            % Combine all key value pairs of one timestep to one line
            lines = join(lines, ',');
            % Use regexprep to replace multiple commas with a single comma
            lines = regexprep(lines, ',{2,}', ',');
            % Add the prefix and timestep to the key value pairs
            lines = append(prefix, " ", lines, " ", string(timestamp));
            % combine all separate lines to one long char
            lines = char(join(lines.', newline));
            % Change the placeholders to quotation marks
            lines = replace(lines, '*!*', '"');
        end
    end
    
    methods(Static, Access = private)
        % Format a field
        function str = fieldFmt(key, value)
            if isfloat(value)
                if ~isempty(value) && isfinite(value)
                    str = sprintf('%s=%.8g', key, value);
                else
                    str = '';
                end
            elseif isinteger(value)
                str = sprintf('%s=%ii', key, value);
            elseif ischar(value)
                str = [key '="' Series.safeValue(value) '"'];
            elseif islogical(value)
                str = [key '=' iif(value, 'true', 'false')];
            else
                error('unsupported value type');
            end
        end
        
        % The following functions escape special characters according to:
        % https://docs.influxdata.com/influxdb/v1.8/write_protocols/line_protocol_reference/#special-characters
        function safe = safeValue(value)
            safe = regexprep(value, '["\\]', '\\$0');
        end
        
        function safe = safeKey(key)
            safe = regexprep(key, '[,= ]', '\\$0');
        end
        
        function safe = safeMeasurement(name)
            safe = regexprep(name, '[, ]', '\\$0');
        end
    end
    
end
