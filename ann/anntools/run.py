# run.py
#
# Copyright (C) 2011-2019 Vas Vasiliadis
# University of Chicago
#
# Wrapper script for running AnnTools
#
##
__author__ = 'Vas Vasiliadis <vas@uchicago.edu>'

import sys, os
import time
import driver
import shutil
import boto3
import botocore
from boto3.dynamodb.conditions import Key, Attr

import os, subprocess
import sys

# Import utility helpers
sys.path.insert(1, os.path.realpath(os.path.pardir))
import helpers

# Get configuration
from configparser import SafeConfigParser
config = SafeConfigParser(os.environ)
config.read('ann_config.ini')

# Add utility code here
AwsRegionName = config['aws']['AwsRegionName']
QueueName = config['aws']['QueueName']
TableName = config['aws']['TableName']
### EOF


"""A rudimentary timer for coarse-grained profiling
"""
class Timer(object):
  def __init__(self, verbose=True):
    self.verbose = verbose

  def __enter__(self):
    self.start = time.time()
    return self

  def __exit__(self, *args):
    self.end = time.time()
    self.secs = self.end - self.start
    if self.verbose:
      print(f"Approximate runtime: {self.secs:.2f} seconds")

if __name__ == '__main__':
  # Call the AnnTools pipeline
  if len(sys.argv) > 1:
    with Timer():
      driver.run(sys.argv[1], 'vcf')
      tmp_folder, user_id, job_id, filename = sys.argv[1].split("/")
      file_folder = tmp_folder+"/"+user_id+"/"+job_id+"/"
      filename_without_suffix = filename.split(".")[0]

      # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.upload_file
      s3_results_bucket = 'gas-results'
      s3_key_result_file = 'tliu77/'+user_id+'/'+filename_without_suffix+'.annot.vcf'
      s3_key_log_file = 'tliu77/'+user_id+'/'+filename_without_suffix+'.vcf.count.log'
      complete_time = int(time.time())


      try:
        s3 = boto3.client('s3', region_name='us-east-1',)
        s3.upload_file(file_folder+filename_without_suffix+'.vcf.count.log', s3_results_bucket, s3_key_log_file)
        s3.upload_file(file_folder+filename_without_suffix+'.annot.vcf', s3_results_bucket, s3_key_result_file)
      except botocore.exceptions.UnknownServiceError as e:
        print("UnknownServiceError.")
      except botocore.exceptions.ClientError as e:
        print(e)

      # remove temp folder
      try:
        shutil.rmtree(tmp_folder+"/"+user_id+"/"+job_id+"/")
      except:
        print("Remove failed")

      # update dynamodb
      try:
        dynamo = boto3.resource('dynamodb', region_name='us-east-1')
        table = table = dynamo.Table('tliu77_annotations')
      except boto3.exceptions.ResourceNotExistsError as e:
        print("ResourceNotExistsError.")
      except botocore.exceptions.ClientError as e:
        print("TableNotExistsError.")
      except:
        print("UnknownError.")
      try:
        response = table.update_item(
            Key={
                'job_id': job_id,
            },
            UpdateExpression="set s3_results_bucket = :b, s3_key_result_file=:r, s3_key_log_file=:l, complete_time=:t, job_status=:s",
            ExpressionAttributeValues={
                ':b': s3_results_bucket,
                ':r': s3_key_result_file,
                ':l': s3_key_log_file,
                ':t': complete_time,
                ':s': "COMPLETED",
            },
            ReturnValues="UPDATED_NEW"
        )
      except botocore.exceptions.ClientError as e:
        print("Cannot update db.")
      except:
        print("Unknow Error.")
  else:
    print("A valid .vcf file must be provided as input to this program.")

### EOF