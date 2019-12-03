import pandas as pd
import requests
from datetime import datetime
from google.oauth2 import service_account
import pandas_gbq
from urllib.parse import urlparse

def launch_adslab(event, context):
    credentials = service_account.Credentials.from_service_account_info(
        {
            "type": "service_account",
            "project_id": "LOREM IPSUM",
            "private_key_id": "LOREM IPSUM",
            "private_key": "-----BEGIN PRIVATE KEY-----LOREM IPSUM-----END PRIVATE KEY-----\n",
            "client_email": "LOREM IPSUM",
            "client_id": "LOREM IPSUM",
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://oauth2.googleapis.com/token",
            "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
            "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/adlab-249109%40appspot.gserviceaccount.com"
        },
    )

    projectid = "adlab-249109"

    ov_table_schema = [
 {
   "name": "TIMESTAMP",
   "type": "TIMESTAMP"
 },
 {
   "name": "HOUR",
   "type": "INTEGER"
 },
 {
   "name": "DATE",
   "type": "DATETIME"
 },
 {
   "name": "REGION",
   "type": "STRING"
 },
 {
   "name": "DEVICE",
   "type": "STRING"
 },
 {
   "name": "KEYWORD",
   "type": "STRING"
 },
 {
   "name": "URL",
   "type": "STRING"
 },
 {
   "name": "DOMAIN",
   "type": "STRING"
 },
 {
   "name": "POSITION",
   "type": "INTEGER"
 }
]



    base_url = "https://www.serprobot.com/api/v1/api.php?api_key="
    api_key = "ENTER API KEY"
    region = "www.google.co.uk"
    devices = ["desktop", "mobile"]

    keywords = ["car insurance", "home insurance"]

    SERP_data_df = pd.DataFrame(columns=['TIMESTAMP', 'DATE','HOUR', 'REGION', 'DEVICE', 'KEYWORD', 'URL', 'DOMAIN', 'POSITION'])
    ADS_data_df = pd.DataFrame(columns=['TIMESTAMP', 'DATE','HOUR', 'REGION', 'DEVICE', 'KEYWORD', 'URL', 'DOMAIN', 'POSITION'])

    dateTimeObj = datetime.now()
    dt_no_tz = dateTimeObj.replace(tzinfo=None)
    dt_date = dt_no_tz.date()
    dt_time = dt_no_tz.time()
    dt_hour = dt_time.hour

    url_requests_list = []
    ads_fld_col = []
    serps_fld_col = []

    for keyword in keywords:
        for device in devices:
            url = base_url + api_key + "&action=get_ads&keyword=" + keyword + "&region=" + region + "&device=" + device
            url_requests_list.append(url)
    print(url_requests_list)

    for concat_url in url_requests_list:
        response = requests.get(concat_url)
        data = response.json()

        kw_col = data['keyword']
        serps_col = data['serps']
        ads_col = data['ads']

        if "device=mobile" in concat_url:
            device = "mobile"
        else:
            device ="desktop"

        for serps in serps_col:
            serp_fld = urlparse(serps)[1]
            serps_fld_col.append(serp_fld)

        for ads in ads_col:
            sep = '/'
            ads_path = urlparse(ads)[2]
            ads_fld = ads_path.split(sep, 1)[0]
            ads_fld_col.append(ads_fld)

        i = 0
        while i < len(serps_col):
            print(concat_url)
            SERP_data_df = SERP_data_df.append(
                {'TIMESTAMP':  dt_no_tz,'DATE':  dt_date,'HOUR':  dt_hour, 'REGION': region, 'DEVICE': device, 'KEYWORD': kw_col,
                 'URL': serps_col[i], 'DOMAIN': serps_fld_col[i], 'POSITION': i + 1}, ignore_index=True)
            i += 1

        i = 0

        while i < len(ads_col):
            ADS_data_df = ADS_data_df.append(
                {'TIMESTAMP':  dt_no_tz,'DATE':  dt_date,'HOUR':  dt_hour, 'REGION': region, 'DEVICE': device, 'KEYWORD': kw_col,
                 'URL': ads_col[i], 'DOMAIN': ads_fld_col[i], 'POSITION': i + 1}, ignore_index=True)
            i += 1

    print(SERP_data_df)
    print(ADS_data_df)

    # SERP_data_df.to_csv('serp.csv', sep=',', index=False, encoding='utf-8')
    # ADS_data_df.to_csv('ads.csv', sep=',', index=False, encoding='utf-8')

    pandas_gbq.to_gbq(SERP_data_df, 'ad_lab.demo_serp',
                      project_id=projectid,
                      if_exists='append',
                      progress_bar=True,
                      credentials=credentials,
                      table_schema=ov_table_schema)

    pandas_gbq.to_gbq(SERP_data_df, 'ad_lab.demo_ads',
                      project_id=projectid,
                      if_exists='append',
                      progress_bar=True,
                      credentials=credentials,
                      table_schema=ov_table_schema)

launch_adslab('event', 'context')
