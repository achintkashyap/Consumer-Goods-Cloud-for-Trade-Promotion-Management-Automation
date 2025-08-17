from simple_salesforce import Salesforce
import requests
from common_variable import *

def salesforce_connect():
    '''
    sf_username = salesforce username
    sf_password = salesforce password for the above username
    sf_security_token = salesforce security token for the above username
    domain = 'test' for non-prod and blank or remove for production
    all above parameters are imported from common_variable file, for real world scenario we can integrate key-vault or any other safe place for credentials.
    :return:
    '''
    #create a salesforce session
    sf = Salesforce(username = sf_username , password = sf_password, security_token = sf_security_token , domain = domain)

    #fetch data from salesforce
    sf_data = sf.query_all(sf_query)

    #update sf data in a object for particular field
    data_to_be_updated_dictonary = {'ID': '', 'field1' : 'value1'}
    sf.bulk.<'sf_table_name'>.update(data_to_be_updated_dictonary , batch_size =10000, use_serial = True)

    return

def processing_services_connection():
    """
    To retrieve data from Salesforce Processing Services, you must first generate a bearer access token.
    This token is then used to authenticate and fetch data from the processing services.

    Note: To enable processing services integration, it must be activated in the Salesforce Connected Apps setup.
    The client_id and client_secret are generated at the org level and can be obtained by the Salesforce system administrator from the Connected Apps consumer details.

    client_id ,client_secret = this will be generated at org level and can be fetched by system admin for salesforce from connected apps consumer details.
    token_url = it is also configured within connected apps.Generally it is configured as extension of sf instance url with "/services/oauth2/token?username="
    :return:
    """
    data_payload = {
        "client_id": client_id,
        "client_secret" : client_secret,
        "username" : sf_username,
        "password" : sf_password
    }

    token_uri = token_url + sf_username + "&password=" + sf_password + sf_security_token + "&grant_type=password"
    access_token_processing_service = requests.post(token_uri,data_payload).json()["access_token"]
    bearer_token = "Bearer " + access_token_processing_service
    get_data_uri = ps_server + "service api to be used")
    get_data_ps = requests.get(get_data_uri, headers = {'Authorization' : bearer_token, "Cache-Control": 'no-cache'})
    if get_dara_ps.status_code !=200:
        print("Connection to fetch data failed")
        sys.exit(1)
    try:
        measure_dump = []
        print(get_data_ps.json()["meta"]['total'])
        for x in range(0,get_data_ps.json()["meta"]['page_count']):
            page_url = ps_server + (get_data_ps.json()["meta"]["links"]["first"])[:-1] + str(x)
            get_page_data = requests.get(page_uri, headers = {'Authorization' : bearer_token, "Cache-Control": 'no-cache'})
            measure_dump += get_page_data.json()["measures"]
    return


# It is a good practise to save only 500000 records in one file. Measure dump can be saved into excel file using pandas df.to_excel.

