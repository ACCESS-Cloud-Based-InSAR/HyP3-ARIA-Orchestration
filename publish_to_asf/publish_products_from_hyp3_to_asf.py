import argparse
import datetime
import json
import pathlib

import boto3
import hyp3_sdk


def generate_ingest_message(hyp3_job_dict: dict):
    bucket = hyp3_job_dict['files'][0]['s3']['bucket']
    product_key = pathlib.Path(hyp3_job_dict['files'][0]['s3']['key'])

    return {
        'ProductName': product_key.stem,
        'DeliveryTime': datetime.datetime.now(tz=datetime.timezone.utc).replace(tzinfo=None).isoformat(),
        'ResponseTopic': {
            'Region': 'us-east-1',
            'Arn': 'arn:aws:sns:us-east-1:406893895021:hyp3-ingest-responses',
        },
        'Browse': {
            'Bucket': bucket,
            'Key': str(product_key.with_suffix('.png')),
        },
        'Metadata': {
            'Bucket': bucket,
            'Key': str(product_key.with_suffix('.json')),
        },
        'Product': {
            'Bucket': bucket,
            'Key': str(product_key),
        },
    }


def publish_ingest_message(message_payload: dict, topic_arn: str):
    topic_region = topic_arn.split(':')[3]
    sns = boto3.client('sns', region_name=topic_region)
    sns.publish(
        TopicArn=topic_arn,
        Message=json.dumps(message_payload),
    )


def main(project_name: str, topic_arn: str, hyp3_url: str, job_type: str):
    hyp3 = hyp3_sdk.HyP3(hyp3_url)
    jobs = hyp3.find_jobs(status_code='SUCCEEDED', job_type=job_type, name=project_name)
    print(f'Publishing {len(jobs)} {job_type} jobs from {hyp3_url} to {topic_arn}')
    for job in jobs:
        print(f'Publishing job ID {job.job_id}')
        ingest_message = generate_ingest_message(job.to_dict())
        # TODO check if product exists in CMR and only publish if not
        publish_ingest_message(ingest_message, topic_arn)


# Queries a HyP3 deployment for completed jobs for a given user and project name, then publishes each of those products
# to ASF's ingest pipeline

# assumes credentials for the Earthdata Login user that ran the jobs are in your .netrc file
# assumes you have AWS credentials with permission to publish to the SNS topic
if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    # TODO add help text for each parameter
    parser.add_argument('--hyp3-url', default='https://hyp3-isce.asf.alaska.edu',
                        choices=['https://hyp3-isce.asf.alaska.edu', 'https://hyp3-tibet.asf.alaska.edu'])
    parser.add_argument('--topic-arn',
                        default='arn:aws:sns:us-east-1:406893895021:ingest-test-jobs',
                        choices=['arn:aws:sns:us-east-1:406893895021:ingest-test-jobs'
                                 'arn:aws:sns:us-east-1:406893895021:ingest-prod-jobs'])
    parser.add_argument('--job-type', default='INSAR_ISCE', choices=['INSAR_ISCE', 'INSAR_ISCE_TEST'])
    parser.add_argument('project_name')
    args = parser.parse_args()
    main(args.project_name, args.topic_arn, args.hyp3_url, args.job_type)
