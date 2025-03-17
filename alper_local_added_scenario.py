import requests
import pandas 
import json
import pyodbc
import datetime
from datetime import datetime
import numpy as np

       
url = 'http://localhost:5002'
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
CURRENCY_CODE = df1['Currency'][0]
df['CURRENCY_CODE'] = CURRENCY_CODE
df['DATEID'] = df['CREATE_DATE'].dt.strftime('%Y%m%d')


SEC_REPORTING_FLAG = df3['SEC Reporting'].astype(int)
df1['SEC_REPORTING_FLAG'] = SEC_REPORTING_FLAG
MIFID_II_COMPLIANCE_FLAG = df2['MiFID II Compliance'].astype(int)
df1['MIFID_II_COMPLIANCE_FLAG'] = MIFID_II_COMPLIANCE_FLAG
FINRA_COMPLIANCE_FLAG = df3['FINRA Compliance'].astype(int)
df1['FINRA_COMPLIANCE_FLAG'] = FINRA_COMPLIANCE_FLAG
#df1['InstrumentType'] = df1['InstrumentType'].fillna('default_value')
df1['DATEID'] = df['DATEID']

# Replace empty strings with NaN
df1.replace('', np.nan, inplace=True)

# Replace NaN values with 'default_value'
df1.fillna('UNKNOWN', inplace=True)



#DATABASE BAGLANTISI PYODBC UZERINDEN SAGLANIR
conn = pyodbc.connect(
    #'Data Source Name=odbc_drivers_sql;'
    #'Driver={SQL Server};'
    'Driver={ODBC Driver 17 for SQL Server};'
    'Server=LAPTOP-LLBKM3AD\\SQLEXPRESS;'
    'Database=EDWH;'
    'Trusted_connection=yes;'
)

#STG_CURRENCY_TYPE ICIN ARA TABLO BOSALTILIR
cursor =  conn.cursor()
cursor.execute ("TRUNCATE TABLE DBO.STG_CURRENCY_TYPE")
conn.commit()
cursor.close()

#STG_CURRENCY_TYPE DATASI FLASK APIDEN PARSE EDILEREK ALINIR
cursor1 = conn.cursor()
for index in df1.iterrows():
       cursor1.execute("INSERT INTO DBO.STG_CURRENCY_TYPE(CURRENCY_CODE,ETL_DATE) "
     "values(?,?)", 
                    df1['Currency'][0],
                    datetime.now()
                    )
conn.commit()
cursor1.close()

cursor2 =  conn.cursor()
cursor2.execute("INSERT INTO DBO.DIM_CURRENCY_TYPE SELECT (SELECT  MAX(CURRENCY_NO) +1 FROM DBO.DIM_CURRENCY_TYPE) AS CURRENCY_NO,SCT.CURRENCY_CODE,GETDATE() ETL_DATE FROM DBO.STG_CURRENCY_TYPE SCT WHERE NOT EXISTS (SELECT 1 FROM DBO.DIM_CURRENCY_TYPE DCT WHERE DCT.CURRENCY_CODE =  SCT.CURRENCY_CODE)")
conn.commit()
cursor2.close()
#STG_TRANSACTION_TYPE ICIN ARA TABLO BOSALTILIR
cursor3 =  conn.cursor()
cursor3.execute ("TRUNCATE TABLE DBO.STG_TRANSACTION_TYPE")
conn.commit()
cursor3.close()

cursor4 =  conn.cursor()
for index in df1.iterrows():
       cursor4.execute("INSERT INTO DBO.STG_TRANSACTION_TYPE(TRANSACTION_TYPE,ETL_DATE) "
     "values(?,?)", 
                    df1['TransactionType'][0],
                    datetime.now()
                    )
conn.commit()
cursor4.close()

cursor5 = conn.cursor()
cursor5.execute("INSERT INTO DBO.DIM_TRANSACTION_TYPE SELECT (SELECT  MAX(TRANSACTION_TYPE_ID) +1 FROM DBO.DIM_TRANSACTION_TYPE) AS TRANSACTION_TYPE_ID,SCT.TRANSACTION_TYPE AS TRANSACTION_TYPE_NAME,GETDATE() ETL_DATE FROM DBO.STG_TRANSACTION_TYPE STT WHERE NOT EXISTS (SELECT 1 FROM DBO.DIM_TRANSACTION_TYPE DTT WHERE DTT.TRANSACTION_TYPE_NAME =  STT.TRANSACTION_TYPE)")
conn.commit()
cursor5.close()

#STG_INSTRUMENT_TYPE ICIN ARA TABLO BOSALTILIR
cursor6 =  conn.cursor()
cursor6.execute ("TRUNCATE TABLE DBO.STG_INSTRUMENT_TYPE")
conn.commit()
cursor6.close()

cursor7 =  conn.cursor()
for index in df1.iterrows():
       cursor7.execute("INSERT INTO DBO.STG_INSTRUMENT_TYPE(INSTRUMENT_TYPE,ETL_DATE) "
     "values(?,?)", 
                    df1['InstrumentType'][0],
                    datetime.now()
                    )
conn.commit()
cursor7.close()

cursor8 = conn.cursor()
cursor8.execute("INSERT INTO DBO.DIM_INSTRUMENT_TYPE SELECT (SELECT  MAX(INSTRUMENT_TYPE_ID) +1 FROM DBO.DIM_INSTRUMENT_TYPE) AS INSTRUMENT_TYPE_ID,SCT.INSTRUMENT_TYPE AS INSTRUMENT_TYPE_NAME,GETDATE() ETL_DATE FROM DBO.STG_INSTRUMENT_TYPE STT WHERE NOT EXISTS (SELECT 1 FROM DBO.DIM_INSTRUMENT_TYPE DTT WHERE DTT.INSTRUMENT_TYPE_NAME =  STT.INSTRUMENT_TYPE)")
conn.commit()
cursor8.close()