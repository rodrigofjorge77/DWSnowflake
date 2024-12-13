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

#### The project is designed with the following components:

- **Data Source**: File nike_dt_s3.csv got from https://www.kaggle.com/
- **AWS S3**: Used for storage files
- **Databricks**: Responsible for distributed processing

## Technologies

- Docker
- Terraform
- PySpark

## Getting Started

1. Clone the repository:
    ```bash
    git clone https://github.com/rodrigofjorge77/IaC-Databricks.git
    ```

2. Navigate to the project directory:
    ```bash
    cd Terraform
    ```
3. Run Docker Compose
    ```bash
    docker build -t databricks-terraform-image .
    docker run -dit --name databricks -v ./IaC:/iac databricks-terraform-image /bin/bash
    ```

## License

This project is licensed under the MIT License

