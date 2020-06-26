# views.py
#
# Copyright (C) 2011-2020 Vas Vasiliadis
# University of Chicago
#
# Application logic for the GAS
#
##
__author__ = 'Vas Vasiliadis <vas@uchicago.edu>'

import uuid
import time
import json
from datetime import datetime

import boto3
from boto3.dynamodb.conditions import Key
from botocore.client import Config
from botocore.exceptions import ClientError

from flask import (abort, flash, redirect, render_template, 
  request, session, url_for)

from gas import app, db
from decorators import authenticated, is_premium
from auth import get_profile, update_profile


"""Start annotation request
Create the required AWS S3 policy document and render a form for
uploading an annotation input file using the policy document.

Note: You are welcome to use this code instead of your own
but you can replace the code below with your own if you prefer.
"""
def myError(code, message):
  error = {
    "code":code,
    "status":"error",
    "message":message
  }
  return error

def get_download_url(key):
  try:
    s3 = boto3.client('s3', 
      region_name=app.config['AWS_REGION_NAME'],
      config=Config(signature_version='s3v4'))
  except:
    return None

  bucket = app.config['AWS_S3_RESULTS_BUCKET']
  # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.generate_presigned_post
  try:
      url = s3.generate_presigned_url('get_object',
          Params={
                  'Bucket': bucket,
                  'Key': key
                  },
          ExpiresIn=app.config['AWS_SIGNED_REQUEST_EXPIRATION']
      )
  except ClientError as e:
      return None

  return url

@app.route('/annotate', methods=['GET'])
@authenticated
def annotate():
  # Create a session client to the S3 service
  s3 = boto3.client('s3', 
    region_name=app.config['AWS_REGION_NAME'],
    config=Config(signature_version='s3v4'))

  bucket_name = app.config['AWS_S3_INPUTS_BUCKET']
  user_id = session['primary_identity']

  # Generate unique ID to be used as S3 key (name)
  key_name = app.config['AWS_S3_KEY_PREFIX'] + user_id + '/' + \
    str(uuid.uuid4()) + '~${filename}'

  # Create the redirect URL
  redirect_url = str(request.url) + '/job'

  # Define policy fields/conditions
  encryption = app.config['AWS_S3_ENCRYPTION']
  acl = app.config['AWS_S3_ACL']
  fields = {
    "success_action_redirect": redirect_url,
    "x-amz-server-side-encryption": encryption,
    "acl": acl
  }
  conditions = [
    ["starts-with", "$success_action_redirect", redirect_url],
    {"x-amz-server-side-encryption": encryption},
    {"acl": acl}
  ]

  # Generate the presigned POST call
  # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.generate_presigned_post
  try:
    presigned_post = s3.generate_presigned_post(
      Bucket=bucket_name, 
      Key=key_name,
      Fields=fields,
      Conditions=conditions,
      ExpiresIn=app.config['AWS_SIGNED_REQUEST_EXPIRATION'])
  except ClientError as e:
    app.logger.error(f"Unable to generate presigned URL for upload: {e}")
    return abort(500)
    
  # Render the upload form which will parse/submit the presigned POST
  return render_template('annotate.html', s3_post=presigned_post), 200


"""Fires off an annotation job
Accepts the S3 redirect GET request, parses it to extract 
required info, saves a job item to the database, and then
publishes a notification for the annotator service.

Note: Update/replace the code below with your own from previous
homework assignments
"""
@app.route('/annotate/job', methods=['GET'])
@authenticated
def create_annotation_job_request():
  try:
    s3 = boto3.client('s3',region_name=app.config['AWS_REGION_NAME'])
    sns = boto3.client('sns',region_name=app.config['AWS_REGION_NAME'])
    dynamo = boto3.resource('dynamodb', region_name=app.config['AWS_REGION_NAME'])
    table = dynamo.Table(app.config['AWS_DYNAMODB_ANNOTATIONS_TABLE'])
  except botocore.exceptions.UnknownServiceError as e:
    return myError(500, "UnknownServiceError."), 500
  except boto3.exceptions.ResourceNotExistsError as e:
    return myError(500, "ResourceNotExistsError."), 500
  except botocore.exceptions.ClientError as e:
    return myError(500, "TableNotExistsError."), 500
  except:
    return myError(500, "UnknownError."), 500

  # Get bucket name, key, and job ID from the S3 redirect URL
  s3_inputs_bucket = str(request.args.get('bucket'))
  s3_key_input_file = str(request.args.get('key'))

  # Extract the job ID from the S3 key
  if not s3_inputs_bucket or not s3_key_input_file:
    return myError(500, "MissingArguments."), 500

  try:
    cnet_id, user_id, file_full_name = s3_key_input_file.split("/")
    job_id, input_file_name = file_full_name.split("~")
  except:
    return myError(500, "MissingArguments."), 500

  # Create a job item and persist it to the annotations database
  # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html
  submit_time = int(time.time())
  job = {
    "job_id":str(job_id),
    "user_id":user_id,
    "input_file_name":input_file_name,
    "s3_inputs_bucket":s3_inputs_bucket,
    "s3_key_input_file":s3_key_input_file,
    "submit_time":submit_time,
    "job_status":"PENDING",
  }
  try:
    table.put_item(Item=job)
  except botocore.exceptions.ClientError as e:
    return myError(500, "InvalidInputError."), 500

  # send message to sns
  # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sns.html#SNS.Client.publish
  arn = app.config['AWS_SNS_JOB_REQUEST_TOPIC']
  try:
    response = sns.publish(
      TopicArn=arn,
      Message=json.dumps({'default':json.dumps(job)}),
      MessageStructure='json',
    )
  except botocore.errorfactory.InvalidParameterException as e:
    return myError(400, "InvalidParameterException."), 400
  except botocore.errorfactory.NotFoundException as e:
    return myError(404, "InvalidParameterException."), 404
  except:
    return myError(500, "UnknownServerError."), 500
  
  return render_template('annotate_confirm.html', job_id=job_id), 200


"""List all annotations for the user
"""
@app.route('/annotations', methods=['GET'])
@authenticated
def annotations_list():
  try:
    dynamo = boto3.resource('dynamodb', region_name=app.config['AWS_REGION_NAME'])
    table = dynamo.Table(app.config['AWS_DYNAMODB_ANNOTATIONS_TABLE'])
  except boto3.exceptions.ResourceNotExistsError as e:
    return myError(500, "ResourceNotExistsError."), 500
  except botocore.exceptions.ClientError as e:
    return myError(500, "TableNotExistsError."), 500
  except:
    return myError(500, "UnknownError."), 500

  # Get list of annotations to display
  user_id = session['primary_identity']
  # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html#DynamoDB.Client.query
  try:
      response = table.query(
          IndexName = 'user_id-index',
          KeyConditionExpression=Key('user_id').eq(user_id)
      )
  except botocore.exceptions.ClientError as e:
    return myError(500, "ClientError."), 500
  except:
    return myError(500, "UnknownError."), 500

  annotations = response['Items']
  for annotation in annotations:
      annotation['submit_time'] = datetime.fromtimestamp(annotation['submit_time'])

  return render_template('annotations.html', annotations=annotations), 200


"""Display details of a specific annotation job
"""
@app.route('/annotations/<id>', methods=['GET'])
@authenticated
def annotation_details(id):
  try:
    dynamo = boto3.resource('dynamodb', region_name=app.config['AWS_REGION_NAME'])
    table = dynamo.Table(app.config['AWS_DYNAMODB_ANNOTATIONS_TABLE'])
  except boto3.exceptions.ResourceNotExistsError as e:
    return myError(500, "ResourceNotExistsError."), 500
  except botocore.exceptions.ClientError as e:
    return myError(500, "TableNotExistsError."), 500
  except:
    return myError(500, "UnknownError."), 500

  # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html#DynamoDB.Client.query
  try:
    response = table.query(
        KeyConditionExpression=Key('job_id').eq(id)
    )
  except botocore.exceptions.ClientError as e:
    return myError(500, "ClientError."), 500
  except:
    return myError(500, "UnknownError."), 500

  # check if job exists
  try:
    annotation = response['Items'][0]
  except IndexError as e:
    return render_template('error.html', title='404 Not Found', 
      alert_level='Low', message='The job does not exists.'), 404
  if annotation['user_id'] != session['primary_identity']:
    return render_template('error.html', title='403 Forbidden', 
      alert_level='High', message='Not authorized to view this job.'), 403
  annotation['submit_time'] = datetime.fromtimestamp(annotation['submit_time'])
  annotation['complete_time'] = datetime.fromtimestamp(annotation['complete_time'])

  # user profile
  user_profile = get_profile(identity_id=session['primary_identity'])
  free_access_expired = False

  if user_profile.role == "free_user":
    if annotation['archived'] == True:
      free_access_expired = True
  else:
    if "archived" in annotation and annotation['archived'] == True:
      annotation['restore_message'] = "The results files are restoring."

  # generate download url
  download_url = get_download_url(annotation['s3_key_result_file'])
  annotation['result_file_url'] = download_url

  return render_template('annotation_details.html', annotation=annotation, free_access_expired=free_access_expired), 200

"""Display the log file contents for an annotation job
"""
@app.route('/annotations/<id>/log', methods=['GET'])
@authenticated
def annotation_log(id):
  try:
    s3 = boto3.client('s3', region_name=app.config['AWS_REGION_NAME'])
    dynamo = boto3.resource('dynamodb', region_name=app.config['AWS_REGION_NAME'])
    table = dynamo.Table(app.config['AWS_DYNAMODB_ANNOTATIONS_TABLE'])
  except botocore.exceptions.UnknownServiceError as e:
    return myError(500, "UnknownServiceError."), 500
  except boto3.exceptions.ResourceNotExistsError as e:
    return myError(500, "ResourceNotExistsError."), 500
  except botocore.exceptions.ClientError as e:
    return myError(500, "TableNotExistsError."), 500
  except:
    return myError(500, "UnknownError."), 500

  # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html#DynamoDB.Client.query
  try:
    response = table.query(
        KeyConditionExpression=Key('job_id').eq(id)
    )
  except botocore.exceptions.ClientError as e:
    return myError(500, "ClientError."), 500
  except:
    return myError(500, "UnknownError."), 500

  free_access_expired = False

  try:
    annotation = response['Items'][0]
  except IndexError as e:
    return render_template('error.html', title='404 Not Found', 
      alert_level='Low', message='The job does not exists.'), 404
  
  if annotation['user_id'] != session['primary_identity']:
    return render_template('error.html', title='403 Forbidden', 
      alert_level='High', message='Not authorized to view this job.'), 403

  results_bucket = app.config['AWS_S3_RESULTS_BUCKET']
  s3_key_log_file = annotation['s3_key_log_file']

  # get log content
  # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.get_object
  try:
    log_file = s3.get_object(Bucket=results_bucket, Key=s3_key_log_file)
    log_file_contents = log_file['Body'].read().decode()
  except botocore.exceptions.ClientError as e:
    return render_template('error.html', title='404 Not Found', 
      alert_level='Low', message='Cannot view the log.'), 404
  return render_template('view_log.html', jon_id=id, log_file_contents=log_file_contents)


"""Subscription management handler
"""
import stripe

@app.route('/subscribe', methods=['GET', 'POST'])
@authenticated
def subscribe():
  if (request.method == 'GET'):
    # Display form to get subscriber credit card info
    if (session.get('role') == "free_user"):
      return render_template('subscribe.html')
    else:
      return redirect(url_for('profile'))

  elif (request.method == 'POST'):
    # Process the subscription request
    token = str(request.form['stripe_token']).strip()

    # Create a customer on Stripe
    stripe.api_key = app.config['STRIPE_SECRET_KEY']
    try:
      customer = stripe.Customer.create(
        card = token,
        plan = "premium_plan",
        email = session.get('email'),
        description = session.get('name')
      )
    except Exception as e:
      app.logger.error(f"Failed to create customer billing record: {e}")
      return abort(500)

    # Update user role to allow access to paid features
    update_profile(
      identity_id=session['primary_identity'],
      role="premium_user"
    )

    # Update role in the session
    session['role'] = "premium_user"


    # Request restoration of the user's data from Glacier
    # Add code here to initiate restoration of archived user data
    # Make sure you handle files not yet archived!
    try:
      sns = boto3.client('sns',region_name=app.config['AWS_REGION_NAME'])
    except botocore.exceptions.ClientError as e:
      return myError(500, "ClientError."), 500
    except:
      return myError(500, "UnknownError."), 500

    # send notification
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sns.html#SNS.Client.publish
    restore_notification = {
      "user_id": session['primary_identity'],
    }
    restore_arn = app.config['AWS_SNS_JOB_RESTORE_TOPIC']
    try:
      response = sns.publish(
        TopicArn=restore_arn,
        Message=json.dumps({'default':json.dumps(restore_notification)}),
        MessageStructure='json',
      )
    except botocore.errorfactory.InvalidParameterException as e:
      return myError(400, "InvalidParameterException."), 400
    except botocore.errorfactory.NotFoundException as e:
      return myError(404, "InvalidParameterException."), 404
    except:
      return myError(500, "UnknownServerError."), 500

    # Display confirmation page
    return render_template('subscribe_confirm.html', 
      stripe_customer_id=str(customer['id']))


"""Reset subscription
"""
@app.route('/unsubscribe', methods=['GET'])
@authenticated
def unsubscribe():
  # Hacky way to reset the user's role to a free user; simplifies testing
  update_profile(
    identity_id=session['primary_identity'],
    role="free_user"
  )
  return redirect(url_for('profile'))


"""DO NOT CHANGE CODE BELOW THIS LINE
*******************************************************************************
"""

"""Home page
"""
@app.route('/', methods=['GET'])
def home():
  return render_template('home.html')

"""Login page; send user to Globus Auth
"""
@app.route('/login', methods=['GET'])
def login():
  app.logger.info(f"Login attempted from IP {request.remote_addr}")
  # If user requested a specific page, save it session for redirect after auth
  if (request.args.get('next')):
    session['next'] = request.args.get('next')
  return redirect(url_for('authcallback'))

"""404 error handler
"""
@app.errorhandler(404)
def page_not_found(e):
  return render_template('error.html', 
    title='Page not found', alert_level='warning',
    message="The page you tried to reach does not exist. \
      Please check the URL and try again."
    ), 404

"""403 error handler
"""
@app.errorhandler(403)
def forbidden(e):
  return render_template('error.html',
    title='Not authorized', alert_level='danger',
    message="You are not authorized to access this page. \
      If you think you deserve to be granted access, please contact the \
      supreme leader of the mutating genome revolutionary party."
    ), 403

"""405 error handler
"""
@app.errorhandler(405)
def not_allowed(e):
  return render_template('error.html',
    title='Not allowed', alert_level='warning',
    message="You attempted an operation that's not allowed; \
      get your act together, hacker!"
    ), 405

"""500 error handler
"""
@app.errorhandler(500)
def internal_error(error):
  return render_template('error.html',
    title='Server error', alert_level='danger',
    message="The server encountered an error and could \
      not process your request."
    ), 500

### EOF