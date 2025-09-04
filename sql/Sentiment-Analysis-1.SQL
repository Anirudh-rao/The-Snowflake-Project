use role ACCOUNTADMIN;

create or replace network rule api_ninjas_network_access
type = host_port
mode = egress
value_list = ('api.api-ninjas.com')
comment  = 'Network rule to access api-ninjas'
;



create or replace EXTERNAL ACCESS INTEGRATION api_ninjas_access_integration
 allowed_network_rules = (api_ninjas_network_access)
 enabled = true;

grant usage on integration api_ninjas_access_integration to role sysadmin;





create or replace function get_api_data()
returns varchar
language python
runtime_version = 3.9
handler= 'get_data'
packages =('requests')
external_access_integrations = (api_ninjas_access_integration)
as 
$$
import requests

api_url = 'https://api.api-ninjas.com/v1/quotes'
api_key = '<YOUR-NINJA-API-KEY>'

def get_data():
    response  = requests.get(api_url, headers={'X-Api-key':api_key})
    if response.status_code == requests.codes.ok:
        return response.text
    else:
        return "Error:", response.status_code,reponse.text


$$;


Select get_api_data();