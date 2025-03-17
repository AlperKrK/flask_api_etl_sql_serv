import requests
import pandas 
import json
import pyodbc
import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime,timedelta
from airflow.hooks.base import BaseHook




#import numpy as np

def get_load_data():
        return         
url = 'http://host.docker.internal:5002'
header = {"Content-Type":"application/json",
         "Accept-Encoding":"deflate"}

response = requests.get(url,headers=header)
#print(response)

responseData= response.json()
#print('Data alindi : ',responseData)


         
df2 = pandas.json_normalize(responseData["RegulatoryCompliance"]['EU'])
df3 = pandas.json_normalize(responseData["RegulatoryCompliance"]['US'])
df2['MIFID_II_COMPLIANCE_FLAG'] = df2['MiFID II Compliance'].astype(int)
df3['FINRA_COMPLIANCE_FLAG'] = df3['FINRA Compliance'].astype(int)
df3['SEC_REPORTING_FLAG'] = df3['SEC Reporting'].astype(int)
#print("new int:  ",df2['MIFID_II_COMPLIANCE_FLAG'][0])
        

df1 =  pandas.json_normalize(responseData)
df =  pandas.json_normalize(responseData["TransactionDetails"])


TRANSACTION_ID = df1['TransactionID'][0]
df['TRANSACTION_ID'] = TRANSACTION_ID
CREATE_DATE = datetime.fromisoformat(df1['Timestamp'][0])
df['CREATE_DATE'] = CREATE_DATE


SEC_REPORTING_FLAG = df3['SEC Reporting'].astype(int)
df1['SEC_REPORTING_FLAG'] = SEC_REPORTING_FLAG
MIFID_II_COMPLIANCE_FLAG = df2['MiFID II Compliance'].astype(int)
df1['MIFID_II_COMPLIANCE_FLAG'] = MIFID_II_COMPLIANCE_FLAG
FINRA_COMPLIANCE_FLAG = df3['FINRA Compliance'].astype(int)
df1['FINRA_COMPLIANCE_FLAG'] = FINRA_COMPLIANCE_FLAG

#print('Transform tamamlandi.')

conna = BaseHook.get_connection('sqlserver_alper')
    
    # Construct the connection string
conn_str = (
        f"DRIVER={conna.extra_dejson.get('driver', 'ODBC Driver 17 for SQL Server')};"
        f"SERVER={'LAPTOP-LLBKM3AD\\SQLEXPRESS'};"
        f"DATABASE={'EDWH'}';"
        f"UID={conna.login};"
        f"PWD={conna.password}"
    )

conn = pyodbc.connect(conn_str,timeout=30)


cursor2 = conn.cursor()
for index in df.iterrows():
            cursor2.execute("INSERT INTO dbo.ODS_TRANSACTION_DETAIL_DATA(TRANSACTION_ID,CLIENTACCOUNT,EXECUTIONPRICE,ORDERPRICE,ORDERTYPE,ORDERQUANTITY,CREATE_DATE,ETL_DATE)"
                "values(?,?,?,?,?,?,?,?)", 
                    df['TRANSACTION_ID'][0],
                    df['ClientAccount'][0],
                    df['ExecutionPrice'][0],
                    int(df['OrderPrice'][0]),
                    df['OrderType'][0],
                    int(df['OrderQuantity'][0]),
                    df['CREATE_DATE'][0],
                    datetime.now()
                    
                    )
conn.commit()
cursor2.close()

cursor = conn.cursor()

for index in df1.iterrows():
     cursor.execute("INSERT INTO dbo.ODS_TRANSACTION_DATA(TRANSACTION_ID,TRANSACTION_TYPE,COUNTERPARTY,COUNTERPARTY_LOCATION,CURRENCY,INSTRUMENTNAME,INSTRUMENTTYPE,MIFID_II_COMPLIANCE_FLAG,FINRA_COMPLIANCE_FLAG,SEC_REPORTING_FLAG,TRANSACTION_AMOUNT,CREATE_DATE,ETL_DATE) "
     "values(?,?,?,?,?,?,?,?,?,?,?,?,?)", 
                    df1['TransactionID'][0],
                    df1['TransactionType'][0],
                    df1['Counterparty'][0],
                    df1['CounterpartyLocation'][0],
                    df1['Currency'][0],
                    df1['InstrumentName'][0],
                    df1['InstrumentType'][0],
                    int(df1['MIFID_II_COMPLIANCE_FLAG'][0]),
                    int(df1['FINRA_COMPLIANCE_FLAG'][0]),
                    int(df1['SEC_REPORTING_FLAG'][0]),
                    float(df1['TransactionAmount'][0]),
                    datetime.fromisoformat(df1['Timestamp'][0]),
                    datetime.now()
                    )
conn.commit()
cursor.close()



dag = DAG(
   'alper_deneme',
    default_args={'start_date': days_ago(1)},
    schedule_interval = timedelta(minutes=10),
    catchup=False
)


print_get_load_data_task = PythonOperator(
    task_id='get_load_data',
    python_callable=get_load_data,
    dag=dag
)


print_get_load_data_task

#df1.to_sql(name='Alper1',con=conn,if_exists='replace',index=False)






#df2 =  pandas.DataFrame(responseData)
#print(df2)

