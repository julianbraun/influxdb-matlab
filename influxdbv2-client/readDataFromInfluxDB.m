
function data = readDataFromInfluxDB(url, org, token, fluxQuery)
    % Define the URL for the query
    endpoint = strcat(url, '/api/v2/query?org=', org);
    
    % Set up the options for webread (HTTP POST)
    options = weboptions('HeaderFields', {'Authorization', ['Token ', token]; 'Accept', 'application/csv'}, 'MediaType', 'application/json');
    
    % Prepare query as a structure
    body = struct('query', fluxQuery);
    
    % Send the query
    result = webwrite(endpoint, body, options);
    
    % Convert the result CSV data into a table
    data = readtable(result);
    disp('Data read from InfluxDB successfully.');
end