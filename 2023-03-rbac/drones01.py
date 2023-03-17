# Read an Excel file, convert it to Parquet, and write it to S3 (or lakeFS)
# -------------------------------------------------------------------------
# Thanks chatGPT for writing this for me 🤖
# plus lakeFS docs (https://docs.lakefs.io/integrations/python.html)
#
# @rmoff
# 2023-03-08
import os
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import boto3

# lakeFS
access_key_id = 'AKIAxxxxxxxxxxxxxxxx'
secret_access_key = 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'

# Set up S3 client
s3 = boto3.client('s3', 
                  endpoint_url='https://my-host.east-2.lakefscloud.io/',
                  aws_access_key_id=access_key_id, 
                  aws_secret_access_key=secret_access_key)

# Set up local file paths
input_folder = './9-28-22 MR132943/FOIA 2022-10356/'
output_folder = input_folder

# Set up S3 bucket and key
bucket = 'drones03'
branch= 'main'
key = 'drone-registrations'

# Iterate over files in input folder
for filename in os.listdir(input_folder):
    if filename.endswith('.xlsx'):
        # Read in XLSX file with Pandas
        print(f'Reading {os.path.join(input_folder, filename)}')
        df = pd.read_excel(os.path.join(input_folder, filename))

        # Write data to parquet file with Pyarrow
        output_filename = os.path.join(output_folder, f'{filename[:-5]}.parquet')
        print(f'Writing parquet to {output_filename}')
        df.to_parquet(output_filename)

        # Upload parquet file to S3
        print(f'Uploading parquet to {bucket}/{branch}/{key}/{filename[:-5]}.parquet\n\n')
        s3.upload_file(output_filename, bucket, f'{branch}/{key}/{filename[:-5]}.parquet')
