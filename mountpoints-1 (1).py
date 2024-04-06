# Databricks notebook source
# DBTITLE 1,Mountpoints For Old Containers
silver = '/mnt/usafactssilver/silver/'
bronze = '/mnt/usafactsbronze/bronze/'
bronze_http = 'https://usafactsbronze.blob.core.windows.net/bronze/'
bronze_ingestion_meta = '/mnt/usafactsbronze/bronze/https_usafactsbronze.blob.core.windows.net/ingestion-meta/'
bronze_meta_info='https_usafactsbronze.blob.core.windows.net/meta_info/'

# COMMAND ----------

# MAGIC %pip install azure-storage-blob

# COMMAND ----------

import re
import os
import requests
import pandas as pd
import numpy as np
import urllib.parse
import urllib.request
from urllib.parse import urlparse
import tempfile
import zipfile
import urllib
from bs4 import BeautifulSoup
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from azure.storage.blob import BlobServiceClient
from datetime import datetime
blob_container_name = 'bronze'
blob_sas_token = dbutils.secrets.get(scope = "usafacts-data-scope", key = "usafactsbronze-access-key")
blob_service_client = BlobServiceClient(account_url=f'https://usafactsbronze.blob.core.windows.net/', credential=blob_sas_token)
container_client = blob_service_client.get_container_client(blob_container_name)
run_datetime  = datetime.now()

# COMMAND ----------

# DBTITLE 1,Mountpoints for Manual storage Containers (to_wild, to_bronze, ingestion_meta)

manual_to_wild = '/mnt/usafactsdpmanual/to-wild/' #dbfs:/mnt/usafactsdpmanual
manual_to_bronze = '/mnt/usafactsdpmanual/to-bronze/'
manual_ingestion_meta='/mnt/usafactsdpmanual/ingestion-metainfo/'

MANUAL_WILD_TO_RAW_CONTAINER = "to-wild"
MANUAL_RAW_TO_BRONZE_CONTAINER = "to-bronze"
MANUAL_INGESTION_META_CONTAINER="ingestion-metainfo"

manual_blob_sas_token = dbutils.secrets.get(scope = "usafacts-data-scope", key = "usafactsdpmanual-access-key")
manual_blob_service_client = BlobServiceClient(account_url=f'https://usafactsdpmanual.blob.core.windows.net/', credential=manual_blob_sas_token)

manual_raw_container_client = manual_blob_service_client.get_container_client(MANUAL_WILD_TO_RAW_CONTAINER)
manual_bronze_container_client = manual_blob_service_client.get_container_client(MANUAL_RAW_TO_BRONZE_CONTAINER)
manual_ingestion_meta_container_client = manual_blob_service_client.get_container_client(MANUAL_INGESTION_META_CONTAINER)

manual_ingestion_meta_source_urls = '/mnt/usafactsdpmanual/ingestion-metainfo/source_urls/'
manual_ingestion_meta_scraped_urls = '/mnt/usafactsdpmanual/ingestion-metainfo/scraped_urls/'

