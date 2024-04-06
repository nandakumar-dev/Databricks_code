# Databricks notebook source
silver = '/mnt/usafactssilver/silver/'
bronze = '/mnt/usafactsbronze/bronze/'
bronze_http = 'https://usafactsbronze.blob.core.windows.net/bronze/'
bronze_ingestion_meta = '/mnt/usafactsbronze/bronze/https_usafactsbronze.blob.core.windows.net/ingestion-meta/'
bronze_meta_info='https_usafactsbronze.blob.core.windows.net/meta_info/'

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
