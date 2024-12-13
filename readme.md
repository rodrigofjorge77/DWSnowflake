# DW Snowflake - Arquitetura Medalhão

## Table of Contents
- [Overview](#overview)
- [System Architecture](#system-architecture)
- [Technologies](#technologies)
- [Getting Started](#getting-started)
- [License](#license)

## Overview

Neste projeto:  
✅ Desenvolvi toda a arquitetura do DW, integrando diferentes camadas para organizar e otimizar o fluxo de dados.  
✅ Modelei o banco de dados no formato dimensional snowflake, utilizando como base o clássico Northwind Model Database da Microsoft.  
✅ Construí pipelines ETL/ELT no Apache Airflow, orquestrando todo o processo em containers Docker.  
✅ Carreguei e organizei os dados no banco Snowflake, explorando toda sua escalabilidade e performance.  
✅ Finalizei com a criação de um dashboard interativo no Power BI, onde os insights ganham vida!  

## System Architecture

![System Architecture](https://github.com/rodrigofjorge77/DWSnowflake/blob/main/Assets/arquitetura.png)

## Data Source Model in Postgres

![System Architecture](https://github.com/rodrigofjorge77/DWSnowflake/blob/main/Assets/schema%20das%20tabelas%20na%20orgiem.png)

## Airflow Pipeline

![System Architecture](https://github.com/rodrigofjorge77/DWSnowflake/blob/main/Assets/Airflow_Full-Load.png)

## Star Schema Snowflake Model

![System Architecture](https://github.com/rodrigofjorge77/DWSnowflake/blob/main/Assets/DW%20Snowflake%20Model.png)

## Power BI Dashboard

![System Architecture](https://github.com/rodrigofjorge77/DWSnowflake/blob/main/Assets/DW%20Dashboard%20PowerBI.png)

## Technologies

- Docker
- Postgres
- Airflow
- Snowflake
- PowerBI

## Getting Started

1. Clone the repository:
    ```bash
    git clone https://github.com/rodrigofjorge77/DWSnowflake.git
    ```

2. Install Airflow:  

    https://airflow.apache.org/docs/apache-airflow/stable/installation/index.html
   
3. Create an Account in Snowflake  
    https://signup.snowflake.com/?utm_cta=trial-en-www-homepage-top-right-nav-ss-evg&_ga=2.18470073.1079222840.1734114122-480247440.1733135053

## License

This project is licensed under the MIT License

