FROM apache/airflow:latest

USER root

RUN apt-get update && \
    apt-get -y install git && \
    apt-get clean
RUN apt-get update
RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
RUN curl https://packages.microsoft.com/config/debian/11/prod.list > /etc/apt/sources.list.d/mssql-release.list 
    
    
RUN exit
RUN apt-get update
RUN env ACCEPT_EULA=Y apt-get install -y msodbcsql17
RUN apt-get update
RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
RUN curl https://packages.microsoft.com/config/debian/11/prod.list > /etc/apt/sources.list.d/mssql-release.list 


RUN exit
RUN apt-get update
RUN env ACCEPT_EULA=Y apt-get install -y msodbcsql17
   
USER airflow




