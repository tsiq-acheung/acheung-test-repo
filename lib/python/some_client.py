"""
For all S3 objects in the terraform buckets in an account, output object key and version ID
"""
import logging

import boto3
import click

logger = logging.getLogger()
logging.basicConfig(level=logging.INFO)


def write_metadata(item, file):
    file.write(get_metadata(item)+"\n")


def get_metadata(item):
    return f"{item['Key'],item['VersionId']}"


def include(item):
    """Only include latest object versions. Don't include apply logs"""
    return item["IsLatest"] and not item["Key"].startswith("apply_logs")


@click.command()
@click.option("--account-id", "-a", required=True)
@click.option("--region", "-r", required=True)
@click.option("--dry-run/--no-dry-run", default=False)
def record_tf_state(account_id, region, dry_run):
    s3_bucket = f"tsiq-terraform-{account_id}-{region}"
    client = boto3.client("s3", region_name=region)
    paginator = client.get_paginator("list_object_versions")
    page_iterator = paginator.paginate(Bucket=s3_bucket)

    with open(f"{s3_bucket}.txt", "w+") as f:
        for page in page_iterator:
            versions = page["Versions"]
            if dry_run:
                latest_metadata = [get_metadata(item) for item in versions if include(item)]
                logger.info(latest_metadata)
                break

            for version in [version for version in versions if version["IsLatest"] and include(version)]:
                write_metadata(version, f)

def restore_object(client, bucket, key, version_id):
    client.copy_object(Bucket=bucket, Key=key, CopySource={
        'Bucket': bucket,
        'Key': key,
        'VersionId': version_id
    })

def restore_tf_state(account_id, region, restore_file):
    s3_bucket = f"tsiq-terraform-{account_id}-{region}"
    client = boto3.client("s3", region_name=region)
    with open(f"{restore_file}", "r") as f:
        objects = f.readlines()
        versions_to_restore = [eval(obj.rstrip("\n")) for obj in objects]
    
    for obj in versions_to_restore:
        restore_object(client, s3_bucket, obj[0], obj[1])

if __name__ == '__main__':
    pass
