# flask_api_etl_sql_serv

flask api etl sql server airflow



Cevaplar :

 #################### 1. Get the real-time financial transaction from the API. #################### 

1. Derleyici ortamı olarak Visual Studio Code kullanılmıştır.
2. DAGS tarafında yükleme hatası alındığı için 4.madde ve otomatik schedule kısmında ilerlenilmemiştir.
3. Mimari olarak FlaskAPI pyhton kodları docker container üzerinden http://localhost:5002 portunda ayaktadır.Burada Data ilk olarak okunup sonrasında on prem local masaüstüne kurulu MS-SQLSERVEREXPRESS sunucusunda SQL SERVER EDWH olarak isimlendirdiğim database üzerine API'den gelen verileri veri ambarı katmanlarına göre almaktadır.

    FLASKAPI>>PYODBC bağlantısı>>SQL Server Express >>EDWH veri tabanı>>Tablolar olarak ilerlenilmektedir.

4.Caselere göre 1,2,3 no'lu isimlendirebileceğimiz kısımlar burada yapılmaktadır.
5.alper_local_test.py dosyası üzerinden dockerda ayağa kalkan flask apiye erişilmektedir.

 #################### 2. Store the data with SCD Type2.  #################### 

Burada mimari olarak API'den gelen datalar üzerinden bir yapı kurmaya çalıştım.Dimension olabilecek alanlar Currency,TransactionType ve InstrumentType alanları olduğu gördüm,diğer alanlarda sabit değişkenler verildiğinden önemsenmemiştir.

SurrogateKEY(SK) yapısını gelen dataya göre Currency||TransactionType||InstrumentType alanlarından oluşturdum.API'den gelen veri Transaction_Id bazında unique olup oluşturulan senaryoya göre katmanlarda hesapladığım SurrogateKEY eğer yoksa insert eğer varsa History oluşturacak şekilde EFF_FROM_DATE,EFF_TO_DATE,IS_CURRENT alanları üzerinden kontrol edilmektedir. 

SK alanı = CONTRACT_NO olarak kolon isimlendirilmesi yapılmıştır.

SK ya konu edilebilecek DIMENSION tablolar ;

DIM_CURRENCY_TYPE(CURRENCY_NO,CURRENCY_CODE,ETL_DATE)
DIM_TRANSACTION_TYPE(TRANSACTION_TYPE_ID,TRANSACTION_TYPE_NAME,ETL_DATE)
DIM_INSTRUMENT_TYPE(INSTRUMENT_TYPE_ID,INSTRUMENT_TYPE_NAME,ETL_DATE)


Senaryolar;

1.Mimari olarak akış >>  ODS_TRANSACTION_DATA>>ODS_TRANSACTION_DETAIL_DATA>>FACT_CONTRACT_DETAIL>>STG_TRANSACTION_DATA>>FACT_CONTRACT_TRANSACTION olarak ilerlemektedir.

2.SurrogateKEY olarak isimlendirdiğimiz CONTRACT_NO kolonu STG_TRANSACTION_DATA tablosunda hesaplanır,ODS_TRANSACTION_DATA tablosu daha önce oluşturduğumuz DIM_CURRENCY_TYPE,DIM_TRANSACTION_TYPE,DIM_INSTRUMENT_TYPE tablolarından key alanına konu edilen CURRENCY_CODE,INSTRUMENT_TYPE_ID,TRANSACTION_TYPE_ID a göre alınır;

INSERT INTO dbo.STG_TRANSACTION_DATA SELECT CONVERT(VARCHAR(8),T.CREATE_DATE,112) AS DATEID,
CONCAT(CONCAT(A.CURRENCY_NO,'',B.INSTRUMENT_TYPE_ID),'',C.TRANSACTION_TYPE_ID) AS CONTRACT_NO, --SURROGATEKEY
T.TRANSACTION_ID,C.TRANSACTION_TYPE_ID,T.TRANSACTION_TYPE,
T.COUNTERPARTY,T.COUNTERPARTY_LOCATION,T.CURRENCY,
T.INSTRUMENTNAME,B.INSTRUMENT_TYPE_ID,T.INSTRUMENTTYPE,
T.MIFID_II_COMPLIANCE_FLAG,T.FINRA_COMPLIANCE_FLAG,
T.SEC_REPORTING_FLAG,T.TRANSACTION_AMOUNT,
(T.TRANSACTION_AMOUNT*DTA.TL_EXCHANGE_RATE) AS TL_AMOUNT,
(T.TRANSACTION_AMOUNT*DTA.USD_EXCHANGE_RATE) AS USD_AMOUNT,
T.CREATE_DATE,GETDATE() EFF_FROM_DATE,NULL AS EFF_TO_DATE,'Y' AS IS_CURRENT,
GETDATE() ETL_DATE
 FROM (SELECT A.* FROM (SELECT DT.*, ROW_NUMBER() OVER(ORDER BY DT.ETL_DATE DESC) AS RNK FROM ODS_TRANSACTION_DATA DT) A WHERE A.RNK = '1') T 
 LEFT JOIN  DBO.DIM_CURRENCY_TYPE A ON T.CURRENCY = A.CURRENCY_CODE 
 LEFT JOIN DBO.DIM_INSTRUMENT_TYPE B ON B.INSTRUMENT_TYPE_NAME =  T.INSTRUMENTTYPE 
 LEFT JOIN DBO.DIM_TRANSACTION_TYPE C ON C.TRANSACTION_TYPE_NAME =  T.TRANSACTION_TYPE 
 LEFT JOIN (SELECT * FROM DBO.DIM_EXCHANGE_RATE 
WHERE DATEID=  CONVERT(VARCHAR(8),GETDATE(),112)) 
DTA ON DTA.DATEID = T.DATEID AND DTA.CURRENCY_CODE=T.CURRENCY
;

3.Eğer SK nihai tablomuz olan FACT_CONTRACT_TRANSCACTION tablosunda yoksa EFF_FROM_DATE = GETDATE() olarak alınır,EFF_TO_DATE  NULL ve IS_CURRENT= 'Y' olarak adreslenerek FACT_CONTRACT_TRANSACTION tablosuna yazdırılır.

INSERT INTO DBO.FACT_CONTRACT_TRANSACTION 
SELECT SRC.DATEID,SRC.CONTRACT_NO,
SRC.TRANSACTION_ID,SRC.TRANSACTION_TYPE_ID,
SRC.TRANSACTION_TYPE,SRC.COUNTERPARTY,
SRC.COUNTERPARTY_LOCATION,SRC.CURRENCY,
SRC.INSTRUMENTNAME,SRC.INSTRUMENT_TYPE_ID,
SRC.INSTRUMENTTYPE,SRC.MIFID_II_COMPLIANCE_FLAG,
SRC.FINRA_COMPLIANCE_FLAG,SRC.SEC_REPORTING_FLAG,
SRC.TRANSACTION_AMOUNT,SRC.TL_AMOUNT,SRC.USD_AMOUNT,
SRC.CREATE_DATE,GETDATE() EFF_FROM_DATE,NULL AS EFF_TO_DATE,'Y' AS IS_CURRENT,GETDATE() ETL_DATE 
FROM DBO.STG_TRANSACTION_DATA SRC 
WHERE  NOT EXISTS (SELECT 1 FROM DBO.FACT_CONTRACT_TRANSACTION TRG WHERE TRG.CONTRACT_NO =  SRC.CONTRACT_NO)
;

4.Eğer SK nihai tablomuz olan FACT_CONTRACT_TRANSACTION tablosunda varsa mevcut olan SK nihai tabloda update edilir. EFF_TO_DATE = GETDATE() ve IS_CURRENT = 'N' olarak adreslenerek tutulur.FACT_CONTRACT_TRANSACTION tablosu hareket tablosu olduğundan güncellenecek her bir SK kaydının en son yüklenen satırı alınarak update edilmektedir.

UPDATE DBO.FACT_CONTRACT_TRANSACTION  SET IS_CURRENT = 'N',
EFF_TO_DATE= GETDATE() WHERE CONTRACT_NO IN  (SELECT S.CONTRACT_NO FROM DBO.STG_TRANSACTION_DATA S JOIN DBO.FACT_CONTRACT_TRANSACTION T ON S.CONTRACT_NO = T.CONTRACT_NO WHERE T.IS_CURRENT = 'Y' AND S.TRANSACTION_ID <> T.TRANSACTION_ID )AND IS_CURRENT = 'Y'

5.Güncel olan kayıt update edilen kaydın üzerine yazılarak history devam ettirilir;

INSERT INTO DBO.FACT_CONTRACT_TRANSACTION 
SELECT SRC.DATEID,SRC.CONTRACT_NO,SRC.TRANSACTION_ID,
SRC.TRANSACTION_TYPE_ID,SRC.TRANSACTION_TYPE,
SRC.COUNTERPARTY,SRC.COUNTERPARTY_LOCATION,
SRC.CURRENCY,SRC.INSTRUMENTNAME,
SRC.INSTRUMENT_TYPE_ID,SRC.INSTRUMENTTYPE,
SRC.MIFID_II_COMPLIANCE_FLAG,SRC.FINRA_COMPLIANCE_FLAG,
SRC.SEC_REPORTING_FLAG,
SRC.TRANSACTION_AMOUNT,
SRC.TL_AMOUNT,SRC.USD_AMOUNT,
SRC.CREATE_DATE,SRC.EFF_FROM_DATE,
NULL AS EFF_TO_DATE,SRC.IS_CURRENT,
GETDATE() AS ETL_DATE FROM DBO.STG_TRANSACTION_DATA SRC 
JOIN (SELECT HH.CONTRACT_NO,HH.TRANSACTION_AMOUNT FROM (SELECT CONTRACT_NO,TRANSACTION_AMOUNT,ROW_NUMBER() OVER(PARTITION BY CONTRACT_NO ORDER BY ETL_DATE DESC) AS RNK FROM DBO.FACT_CONTRACT_TRANSACTION WHERE IS_CURRENT = 'N')HH WHERE HH.RNK = 1) TRG 
ON SRC.CONTRACT_NO = TRG.CONTRACT_NO AND SRC.TRANSACTION_ID <> TRG.TRANSACTION_ID
;

 #################### 3.Add some mostly common data transformations  ####################

  #################### 3.1 Data Cleaning  ####################

  Burada ilk 2 senaryoda oluşturduğum INSTRUMENT_TYPE node'undan gelen veride null veriler vardı bunlar Unknown olarak alınmıştır.

  #################### 3.2 Currency Conversion  ####################

  Burada flask apiden gelen ham data zenginleştirilerek FACT_CONTRACT_TRANSACTION tablosuna TL_AMOUNT,USD_AMOUNT kolonları eklenmiştir.DIM_EXCHANGE_RATE manuel 
  oluşturduğum tabloya giderek FACT_CONTRACT_TRANSACTION tablosundaki TRANSACTION_AMOUNT ile TL_EXCHANGE_RATE değerinin çarpımı >>TL_AMOUNT , USD_EXCHANGE_RATE ile çarpımı 
  >>USD_AMOUNT kolonlarında hesaplanmaktadır. 

  #################### 3.3 Regulatory Compliance Enrichment:  ####################
  
  Burada   "RegulatoryCompliance": {
    "EU": {
      "MiFID II Compliance": false
    },
    "US": {
      "FINRA Compliance": true,
      "SEC Reporting": true
    }
  }, olarak API'den gelenler ayrıştırılarak FACT_CONTRACT_TRANSACTION tablosunda MIFID_II_COMPLIANCE_FLAG,FINRA_COMPLIANCE_FLAG,SEC_REPORTING_FLAG kolonlarında tutulmaktadır.Gelen değerler FLAG alanın olduğu için bool gelen değerlerden true değeri yerine 1 false değeri yerine 0 olarak standart oluşturulmuştur.

   #################### 3.4 Complex Transformation Rules:  ####################

  Burada FACT_CONTRACT_DETAIL tablosunda GROSS_MARGIN_TL_AMOUNT >> (EXECUTIONPRICE- 
  ORDERPRICE)*TL_EXCHANGE_RATE*ORDERQUANTITY,GROSS_MARGIN_USD_AMOUNT >> (EXECUTIONPRICE-ORDERPRICE)*USD_EXCHANGE_RATE*ORDERQUANTITY olarak hesaplanmaktadır.

  Ek olarak SCD TYPE 2 devamlılığı için alper_local_added_scenario.py dosyasında eğer yeni bir CURRENCY,TRANSACTION_TYPE ya da INSTRUMENT_TYPE gelirse SurrogateKey 
  yapısının devamlılığı için senaryolar bulabilirsiniz.SQL scriptleri ve test senaryoları için sql_integrity_scenarios altında 3 case için de ayrı ayrı test edilmiştir.
  
#################### 4 Explain your plan for how you plan to solve ####################

Burada airflow hatası aldığımdan dolayı check edemedim.Partition vb db seviyesindeki işler için de yapıyı tekrar kurgulamam gerekebilecek durumlar olabilirdi,o yüzden bu kısmı ilerletemedim.


  

   
   



