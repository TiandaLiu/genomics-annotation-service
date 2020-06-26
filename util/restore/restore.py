# restore.py
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
config.read('restore_config.ini')

# Add utility code here
AwsRegionName = config['aws']['AwsRegionName']
AWS_SQS_JOB_RESTORE_QUEUE = config['aws']['AWS_SQS_JOB_RESTORE_QUEUE']
TableName = config['aws']['TableName']
AWS_SNS_THAW_TOPIC = config['aws']['AWS_SNS_THAW_TOPIC']
AWS_S3_GLACIER_VAULT_NAME = config['aws']['AWS_S3_GLACIER_VAULT_NAME']

def main():
	# config aws service
	# https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs.html#SQS.Client.receive_message
	try:
		s3 = boto3.client('s3',region_name=AwsRegionName)
		glacier = boto3.client('glacier',region_name=AwsRegionName)

		sqs = boto3.resource('sqs',region_name=AwsRegionName)
		queue = sqs.get_queue_by_name(QueueName=AWS_SQS_JOB_RESTORE_QUEUE)

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
				user_id = msg_body['user_id']

				# get archived ids
				# https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html
				try:
					response = table.query(
						IndexName = 'user_id-index',
						KeyConditionExpression=Key('user_id').eq(user_id)
					)
				except botocore.exceptions.ClientError as e:
					print(e)
					return
				items = response['Items']
				archive_ids = []
				for item in items:
					if "results_file_archive_id" in item and item['user_id'] == user_id and item['archived'] == True:
						archive_id = item['results_file_archive_id']
						archive_ids.append(archive_id)

				# send restore request
				# https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glacier.html#Glacier.Client.initiate_job
				for archive_id in archive_ids:
					try:
						response = glacier.initiate_job(
							vaultName=AWS_S3_GLACIER_VAULT_NAME,
							jobParameters={
								'Type': "archive-retrieval",
								'ArchiveId': archive_id,
								'SNSTopic': AWS_SNS_THAW_TOPIC,
								'Tier': 'Expedited',
							}
						)
						print("Expedited resp:", response)
						print("Archive id: ", archive_id)
					except:
						# print("cannot get expedited service.")
						# return
						response = glacier.initiate_job(
							vaultName=AWS_S3_GLACIER_VAULT_NAME,
							jobParameters={
								'Type': "archive-retrieval",
								'ArchiveId': archive_id,
								'SNSTopic': AWS_SNS_THAW_TOPIC,
								'Tier': 'Standard',
							}
						)
						print("Standard resp:", response)
						print("Archive id: ", archive_id)

				# Delete the message from the queue
				print ("Deleting message...")
				message.delete()

if __name__ == "__main__":
	main()

### EOF