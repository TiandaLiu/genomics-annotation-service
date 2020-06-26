# thaw.py
#
# NOTE: This file lives on the Utils instance
#
# Copyright (C) 2011-2019 Vas Vasiliadis
# University of Chicago
##
__author__ = 'Vas Vasiliadis <vas@uchicago.edu>'

import os, sys, json
import boto3
import botocore
from botocore.client import Config
from boto3.dynamodb.conditions import Key, Attr

# Import utility helpers
sys.path.insert(1, os.path.realpath(os.path.pardir))
import helpers

# Get configuration
from configparser import SafeConfigParser
config = SafeConfigParser(os.environ)
config.read('thaw_config.ini')

# Add utility code here
AwsRegionName = config['aws']['AwsRegionName']
AWS_SQS_JOB_THAW_QUEUE = config['aws']['AWS_SQS_JOB_THAW_QUEUE']
TableName = config['aws']['TableName']
AWS_S3_GLACIER_VAULT_NAME = config['aws']['AWS_S3_GLACIER_VAULT_NAME']
AWS_S3_RESULTS_BUCKET = config['aws']['AWS_S3_RESULTS_BUCKET']

def main():
	# config aws service
	# https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs.html#SQS.Client.receive_message
	try:
		s3 = boto3.client('s3',region_name=AwsRegionName)
		glacier = boto3.client('glacier',region_name=AwsRegionName)

		sqs = boto3.resource('sqs',region_name=AwsRegionName)
		queue = sqs.get_queue_by_name(QueueName=AWS_SQS_JOB_THAW_QUEUE)

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

				# extract user_id from msg_body
				print("msg_body: ", msg_body)
				restore_id = msg_body['JobId']
				results_file_archive_id = msg_body['ArchiveId']

				# get job key by archive id
				try:
					response = table.query(
						IndexName='results_file_archive_id-index',
						KeyConditionExpression=Key('results_file_archive_id').eq(results_file_archive_id)
					)
				except botocore.exceptions.ClientError as e:
					print(e)
					return

				try:
					job = response['Items'][0]
				except:
					print("No data")
					return
				s3_key_result_file = job['s3_key_result_file']
				job_id = job['job_id']

				# get job content
				# https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glacier.html#Glacier.Client.get_job_output
				job_resp = glacier.get_job_output(vaultName=AWS_S3_GLACIER_VAULT_NAME, jobId=restore_id)
				job_content = job_resp['body']
				print("content type: ", job_content)

				# restore to s3
				# https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.upload_fileobj
				results_bucket = AWS_S3_RESULTS_BUCKET
				s3.upload_fileobj(job_content, AWS_S3_RESULTS_BUCKET, s3_key_result_file)

				# update database
				# https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html#DynamoDB.Client.update_item
				try:
					response = table.update_item(
						Key={
							'job_id': job_id,
						},
						UpdateExpression="set archived=:a",
						ExpressionAttributeValues={
							':a': False,
						},
						ReturnValues="UPDATED_NEW"
					)
				except botocore.exceptions.ClientError as e:
					print("Cannot update db.")
				except:
					print("Unknow Error.")

				# Delete the message from the queue
				print ("Deleting message...")
				message.delete()

if __name__ == "__main__":
	main()

### EOF