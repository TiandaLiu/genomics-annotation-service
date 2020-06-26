# archive.py
#
# NOTE: This file lives on the Utils instance
#
# Copyright (C) 2011-2019 Vas Vasiliadis
# University of Chicago
##
__author__ = 'Vas Vasiliadis <vas@uchicago.edu>'

import boto3
import botocore
from botocore.client import Config
from boto3.dynamodb.conditions import Key, Attr

import uuid, json
import os, subprocess
import sys

# Import utility helpers
sys.path.insert(1, os.path.realpath(os.path.pardir))
import helpers

# Get configuration
from configparser import SafeConfigParser
config = SafeConfigParser(os.environ)
config.read('archive_config.ini')

# Add utility code here
AwsRegionName = config['aws']['AwsRegionName']
SQS_Archive_Name = config['aws']['SQS_Archive_Name']
TableName = config['aws']['TableName']
Results_Bucket = config['aws']['Results_Bucket']
Glacier_Vault_Name = config['aws']['Glacier_Vault_Name']

def main():
	# config aws service
	# https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs.html#SQS.Client.receive_message
	try:
		s3 = boto3.client('s3',region_name=AwsRegionName)
		glacier = boto3.client('glacier',region_name=AwsRegionName)

		sqs = boto3.resource('sqs',region_name=AwsRegionName)
		queue = sqs.get_queue_by_name(QueueName=SQS_Archive_Name)

		dynamo = boto3.resource('dynamodb', region_name=AwsRegionName)
		table = table = dynamo.Table(TableName)
	except botocore.exceptions.UnknownServiceError as e:
		print("UnknownServiceError.")
		return
	except boto3.exceptions.ResourceNotExistsError as e:
		print("ResourceNotExistsError.")
		return
	except botocore.exceptions.ClientError as e:
		print("TableNotExistsError.")
		return
	except:
		print("UnknownError.")
		return

	while True:
		# Get messages
		messages = queue.receive_messages(
				AttributeNames=['All'],
				MaxNumberOfMessages=10,
				WaitTimeSeconds=5,
				)

		if len(messages) > 0:
			print("Received {0} messages...".format(str(len(messages))))
			# Iterate each message
			for message in messages:
				# Parse JSON message (going two levels deep to get the embedded message)
				sqs_body = json.loads(message.body)
				msg_body = json.loads(sqs_body['Message'])

				# extract job information from msg_body
				job_id = msg_body['job_id']
				user_id = msg_body['user_id']
				s3_key_result_file = msg_body['s3_key_result_file']
				print("jobid: ", job_id)

				# get user profile
				user_profile = helpers.get_user_profile(id = user_id, db_name =config['postgresql']['DB_NAME'])
				print("profile: ", user_profile)
				print(type(user_profile))
				print("role: ", user_profile['role'])
				# premium user
				if user_profile['role'] == "premium_user":
					print ("Deleting message...")
					message.delete()
					continue

				# get result file from s3
				results_bucket = Results_Bucket
				try:
					result_file = s3.get_object(Bucket=results_bucket, Key=s3_key_result_file)
					log_content = result_file['Body'].read()
				except botocore.exceptions.ClientError as e:
					print(e)
					return

				# save file to glacier
				try:
					glacier_vault_name = Glacier_Vault_Name
					response = glacier.upload_archive(vaultName=glacier_vault_name, body=log_content)
					print(response)
					results_file_archive_id = response['ResponseMetadata']['HTTPHeaders']['x-amz-archive-id']
				except:
					print("Cannot upload file to glacier.")
					return

				# update database
				try:
					response = table.update_item(
						Key={
							'job_id': job_id,
						},
						UpdateExpression="set results_file_archive_id =:i, archived=:a",
						ExpressionAttributeValues={
							':i': results_file_archive_id,
							':a': True,
						},
						ReturnValues="UPDATED_NEW"
					)
				except botocore.exceptions.ClientError as e:
					print("Cannot update db.")
				except:
					print("Unknow Error.")

				# delete the result file from s3
				try:
					s3.delete_object(Bucket=results_bucket, Key=s3_key_result_file)
					print("result deleted from s3")
				except ClientError as e:
					print(e) 

				# Delete the message from the queue
				print ("Deleting message...")
				message.delete()


if __name__ == "__main__":
	# create a results folder to store the temp files and results
	if "tmp" not in os.listdir():
		os.makedirs("tmp/")

	# start annotator
	main()

### EOF
