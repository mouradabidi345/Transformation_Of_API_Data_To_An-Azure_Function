import logging
import os
import azure.functions as func
import datetime
import logging

import azure.functions as func
from azure.storage.blob import ContainerClient

import http.client
import json
from io import StringIO
import csv
import pandas as pd
# import xlrd
# import xlsxwriter
# import xlwings as xw
# import openpyxl as pxl
import datetime
from ast import literal_eval
import logging

import azure.functions as func
from azure.storage.blob import ContainerClient
        
        
def Bloomfire(apikey, email):
        d= datetime.datetime.today()
        Mon_offset = (d.weekday() - 0) % 7
        Monday_same_week = d - datetime.timedelta(days=Mon_offset)
        Monday_same_weeks = Monday_same_week.strftime("%Y-%m-%d")
        td = datetime.timedelta(days=7)
        Monday_previous_week = (Monday_same_week - td).strftime("%Y-%m-%d")
        Urlendpoint = "https://reports-api.bloomfire.com/" + "member_engagement/full.csv?" + "date_range=" + Monday_previous_week + "%20to%20" + Monday_same_weeks

   
     

#open http connection to client bloomfire
        conn = http.client.HTTPSConnection("asea-global.bloomfire.com")

# convert payload to json
        payload = json.dumps({
        "api_key": apikey,
        "email": email
        })

#Set headers
        headers =  {
        'Content-Type': 'application/json',
        'Bloomfire-Requested-Fields': 'reports_api_token'
        }
#request connection to api
        conn.request("POST", "/api/v2/login", payload, headers)
#read response
        res = conn.getresponse()
        data = res.read()
#convert from bytes to string
        data = data.decode("utf-8")
        data = json.loads(data)
#close connections that were opened when getting token
        res.close()
        conn.close()

#assign token to bloomfire_token variable
        bloomfire_token = data["reports_api_token"]["token"]

#open another http connection to client bloomfire
        conn1 = http.client.HTTPSConnection("reports-api.bloomfire.com")
        payload1 = ''
        headers1 = {
        'Content-Type': 'application/json',
                'Authorization': 'Bearer' + ' ' + bloomfire_token
            }

#request connection to api
        conn1.request("GET", Urlendpoint, payload1, headers1)

#read response
        res1 = conn1.getresponse()
        data = res1.read()
#conevrt data from bytes to string
        data = data.decode("utf-8")
        res.close()
        conn.close()
#insert the string data into a dataframe
        # print(type(data))
    
        StringData = StringIO(data)
        df = pd.read_csv(StringData, sep =",")
#print(df)
# print(type(df['Date']))
    
        df['Date']= pd.to_datetime(df['Date'])
    # df['Dateint'] = int(df['Date'].strftime("%Y%m%d%H%M%S"))
        print(df["Date"])
    # df['Date'] = pd.to_datetime(df['Date']).dt.strftime('%Y-%m-%d %H:%M:%S')
    # df['Dateint'] = df['Date'].apply(lambda x:x.toordinal())
        df['Dateint'] = df['Date'].apply(lambda x: x.value)

        # print(df['Dateint'])
        df['natural_key'] =  str(df['Email']) + str(df['Dateint']) + str(df['Action'])
        df['natural_key'] =  df['Email'].map(str) + df['Dateint'].map(str) + df['Action'].map(str)
    #remove any space
        df['natural_key']= df['natural_key'].str.replace(' ', '')
        df.drop(df.columns[5], axis=1, inplace = True)
    # print(df)
        # print(df.info())
    # print(type(df))
    
    
        df['Date'] = pd.to_datetime(df['Date']).dt.strftime('%Y-%m-%d %H:%M:%S')
    ##Convert the dataframe to json format

        result = df.to_json(orient="records", date_format='iso')
        parsed = json.loads(result)
        result1 =json.dumps(parsed, indent=4)
        return parsed



def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')
    
    email = 'ITdev@aseaglobal.com'
    time = datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')  
    # api_key = req.params.get('api_key')
    apikey = os.environ["apikey"]
    bloomfire_result = Bloomfire(apikey, email)
    response = {
        "schema": {
            "Boomfire": {
                "primary_key": [
                    "natural_key"
                ]
            }
        },
        "state": {
            "cursor": time
        }
    }



    response['insert'] = {"Bloomfire": bloomfire_result}
        # ########New
        # blob = ContainerClient.from_connection_string(conn_str="DefaultEndpointsProtocol=https;AccountName=relearning;AccountKey=cmWjWdQN+oCYNTpgjw1Ntg7GRFPoGGMGmxXc35wsLtPmefPdQxZkfA+peJQJPO16YsD2dhO7LIyZ+AStRMMdww==;EndpointSuffix=core.windows.net", container_name="wfm")
    data = json.dumps(bloomfire_result, indent = 8)
    print(type(data))
        # blob.upload_blob(data, blob)
        # #############

        # return func.HttpResponse(json.dumps(response))
    return func.HttpResponse(json.dumps(bloomfire_result))
    # else:
    #     return func.HttpResponse(
    #          "This HTTP triggered function executed successfully. Pass a name in the query string or in the request body for a personalized response.",
    #          status_code=200
    #     )


# def main(req: func.HttpRequest) -> func.HttpResponse:
#     logging.info('Python HTTP trigger function processed a request.')
#     name = os.environ["name"]
#     # name = os.getenv("name")
#     return func.HttpResponse(f"Hello, {name}. This HTTP triggered function executed successfully.")
    

