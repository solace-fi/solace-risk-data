# THIS IS AN EXAMPLE ONLY
# We are moving publishing process from spreadsheet to S3 and python data pipeline
import pandas as pd
import json
from pinatapy import PinataPy
import requests
import os
from dotenv import load_dotenv
load_dotenv()
INFURA_1 = os.environ.get("INFURA_1")
INFURA_2 = os.environ.get("INFURA_2")
PINATA_KEY = os.environ.get("PINATA_KEY")
PINATA_SECRET_KEY = os.environ.get("PINATA_SECRET_KEY")

#print(INFURA_1)
xls = pd.ExcelFile('SolaceExploitDatabase.xlsx')

def build_metadata():
    metadataOut =  {
        "seriesName": "Series 1.3",
        "version": "0.0.1",
        "dateCreated": "2021-07-03",
        "provenance": "ipfs://cid0000000",
        "generatedBy": "SolacePublish SDK 0.1 Exporter",
        "sourceDataType": "csv",
        "sourceDataVersion": "0.0.1"
    }
    return metadataOut

def build_function():
    functionOut = {
        "name": "getScores",
        "description": "rating engine for soteria",
        "provenance": "ipfs://cid0000001",
    }
    return functionOut

# protocolMap
sheet1 = xls.parse(2) # position of sheet in excel file (0-indexed)
protocolMap = sheet1[["appId","category","tier"]] # must match column names in excel file
protocolMapObj = [{k: v for k, v in x.items() if pd.notnull(v)} for x in protocolMap.to_dict('records')]

# corrValue
corrValue = xls.parse(3) # position of sheet in excel file (0-indexed)
corrValueObj = [{k: v for k, v in x.items() if pd.notnull(v)} for x in corrValue.to_dict('records')]

# correlCat
correlCat = xls.parse(4) # position of sheet in excel file (0-indexed)
correlCatObj = [{k: v for k, v in x.items() if pd.notnull(v)} for x in correlCat.to_dict('records')]

# rateTable
rateCard = xls.parse(5) # position of sheet in excel file (0-indexed)
rateCardObj = [{k: v for k, v in x.items() if pd.notnull(v)} for x in rateCard.to_dict('records')]

outputSeries = json.dumps({"metadata":build_metadata(), "function":build_function(),"data":{ "protocolMap": protocolMapObj, "corrValue": corrValueObj, "correlCat": correlCatObj, "rateCard": rateCardObj}},indent=2)
print(outputSeries)
files = {'upload_file': ('series.json', outputSeries, 'application/json')}

pinata = PinataPy(PINATA_KEY, PINATA_SECRET_KEY)
pinata.pin_json_to_ipfs(outputSeries)
#response = requests.post('https://ipfs.infura.io:5001/api/v0/add', files=files, auth=('20yHOetO97I3C08kF45C428ylKW','e6259ddaa8f308948cbf97b7017c42ce'))
#response = requests.post('https://ipfs.infura.io:5001/api/v0/add', files=files, auth=(INFURA_1, INFURA_2))
#Qm = response.content.decode('utf-8').split('"')[7]
#print(Qm)

# Put to S3
#import json
import boto3
#from datetime import datetime
#import requests
from dotenv import load_dotenv
load_dotenv()
ACCESS_KEY = os.environ.get("ACCESS_KEY")
SECRET_KEY = os.environ.get("SECRET_KEY")
DATA_BUCKET = os.environ.get("DATA_BUCKET")

s3_client = boto3.client("s3", region_name="us-west-2", aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY)
s3_cache = {}
def s3_put(key, body):
    s3_client.put_object(Bucket=DATA_BUCKET, Body=body, Key=key)

# retrieves an object from S3, optionally reading from cache for testing
def s3_get(key, cache=False):
    if cache and key in s3_cache:
        return s3_cache[key]
    else:
        res = s3_client.get_object(Bucket=DATA_BUCKET, Key=key)["Body"].read().decode("utf-8").strip()
        s3_cache[key] = res
        return res


s3_put("current-rate-data/series.json", outputSeries)
# Confirm/test using swagger https://app.swaggerhub.com/apis/i001962/solace-risk_api_alpha/0.0.1#/Balances/getBalances