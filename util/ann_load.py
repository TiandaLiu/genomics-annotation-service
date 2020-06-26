import boto3, botocore
import time, uuid, json


def submit_single_ann_job():
  s3 = boto3.client('s3',region_name='us-east-1')
  sns = boto3.client('sns',region_name='us-east-1')
  dynamo = boto3.resource('dynamodb', region_name='us-east-1')
  table = dynamo.Table('tliu77_annotations')

  job_id = str(uuid.uuid4())
  submit_time = int(time.time())
  s3_key_input_file = 'tliu77/a6ffb8e7-58b9-4a76-8da8-fd63db9383c6/2c21f53f-c9fb-4d3a-af72-e3b00c632120~test.vcf'
  job = {
    "job_id": job_id,
    "user_id": 'a6ffb8e7-58b9-4a76-8da8-fd63db9383c6',
    "input_file_name": 'test.vcf',
    "s3_inputs_bucket": 'gas-inputs',
    "s3_key_input_file": s3_key_input_file,
    "submit_time": submit_time,
    "job_status": "PENDING",
  }

  try:
    table.put_item(Item=job)
  except botocore.exceptions.ClientError as e:
    return myError(500, "InvalidInputError."), 500
  except:
    print("Cannot put item into database.")
    return

  arn = 'arn:aws:sns:us-east-1:127134666975:tliu77_job_requests'
  try:
    response = sns.publish(
      TopicArn=arn,
      Message=json.dumps({'default':json.dumps(job)}),
      MessageStructure='json',
    )
  except:
    print("Error!")
    return

def main():
  while True:
    submit_single_ann_job()
    time.sleep(5)

if __name__ == "__main__":
  main()

# scale policy: 