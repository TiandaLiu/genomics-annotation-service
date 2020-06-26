# notify.py
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

# # Import utility helpers
# sys.path.insert(1, os.path.realpath(os.path.pardir))
# import helpers

# Get configuration
from configparser import SafeConfigParser
config = SafeConfigParser(os.environ)
config.read('ann_config.ini')

# Add utility code here
AwsRegionName = config['aws']['AwsRegionName']
QueueName = config['aws']['QueueName']
TableName = config['aws']['TableName']

def main():
	# config aws service
	# https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs.html#SQS.Client.receive_message
	try:
		sqs = boto3.resource('sqs',region_name=AwsRegionName)
		queue = sqs.get_queue_by_name(QueueName=QueueName)
		s3 = boto3.client('s3',region_name=AwsRegionName)
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
				s3_inputs_bucket = msg_body['s3_inputs_bucket']
				s3_key_input_file = msg_body['s3_key_input_file']
				print("Processing: ", job_id)

				# download file from s3
				# https://boto3.amazonaws.com/v1/documentation/api/latest/guide/s3-example-download-file.html
				filename = s3_key_input_file.split("/")[-1]
				try:
					os.makedirs("tmp/{}/{}".format(user_id, job_id))
					s3.download_file(s3_inputs_bucket, s3_key_input_file, "tmp/{}/{}/".format(user_id, job_id) + filename)
				except botocore.exceptions.ClientError as e:
					if e.response['Error']['Code'] == "404":
						print(404, "File not found.")
						continue
					else:
						print(500, "Server error.")
						continue
				except:
					print("UnknownError.")
					continue

				# dynamodb conditional update
				# https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/GettingStarted.Python.03.html
				try:
					response = table.update_item(
						Key={
							'job_id': job_id,
						},
						UpdateExpression="set job_status = :new_status",
						ConditionExpression="job_status = :old_status",
						ExpressionAttributeValues={
							':old_status': 'PENDING',
							':new_status': 'RUNNING',
						},
						ReturnValues="UPDATED_NEW"
					)
				except botocore.exceptions.ClientError as e:
					if e.response['Error']['Code'] == "ConditionalCheckFailedException":
						print(400, "Bad request."), 400
						continue
					else:
						print(500, "UnknownError."), 500
						continue
				except:
					print("UnknownError.")
					continue

				# spawns a subprocess to run annotator
				try:
					cmd = "python run.py tmp/{}/{}/{}".format(user_id, job_id, filename)
					print(cmd)
					cmd = cmd.split()
					process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
				except:
					print(500, "Cannot run job")
					continue

				# Delete the message from the queue
				print ("Deleting message...")
				message.delete()


if __name__ == "__main__":
	# create a results folder to store the temp files and results
	if "tmp" not in os.listdir():
		os.makedirs("tmp/")

	# start annotator
	main()
