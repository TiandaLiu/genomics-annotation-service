# notify.py
#
# NOTE: This file lives on the Utils instance
#
# Copyright (C) 2011-2019 Vas Vasiliadis
# University of Chicago
##
__author__ = 'Vas Vasiliadis <vas@uchicago.edu>'

import os, sys
import json
import boto3
import botocore

# Import utility helpers
sys.path.insert(1, os.path.realpath(os.path.pardir))
import helpers

# Get configuration
from configparser import SafeConfigParser
config = SafeConfigParser(os.environ)
config.read('notify_config.ini')

# Add utility code here
AwsRegionName = config['aws']['AwsRegionName']
AWS_SQS_JOB_COMPLETE_QUEUE = config['aws']['AWS_SQS_JOB_COMPLETE_QUEUE']
SENDER_EMAIL = config['aws']['SENDER_EMAIL']
JOB_DETAIL_URL_PREFIX = config['aws']['JOB_DETAIL_URL_PREFIX']

def main():
	# config aws service
	# https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs.html#SQS.Client.receive_message
	try:
		sqs = boto3.resource('sqs',region_name=AwsRegionName)
		queue = sqs.get_queue_by_name(QueueName=AWS_SQS_JOB_COMPLETE_QUEUE)
	except botocore.exceptions.UnknownServiceError as e:
		print("UnknownServiceError.")
		return
	except boto3.exceptions.ResourceNotExistsError as e:
		print("ResourceNotExistsError.")
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
				job_detail_url = JOB_DETAIL_URL_PREFIX + job_id

				user_profile = helpers.get_user_profile(id = user_id, db_name =config['postgresql']['DB_NAME'])
				recipient_email = user_profile['email']
				recipient_name = user_profile['name']

				# send email
				sender_email = SENDER_EMAIL
				email_subject = "Job Completed"
				email_body = "Hi {},\n\n    The job {} has completed. Check detail here: {}".format(recipient_name, job_id, job_detail_url)

				# send the email
				helpers.send_email_ses(recipients=recipient_email, sender=sender_email, subject=email_subject, body=email_body)

				# Delete the message from the queue
				print ("Deleting message...")
				message.delete()


if __name__ == "__main__":
	main()