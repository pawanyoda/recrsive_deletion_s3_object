import boto3

# Replace these with your AWS credentials and S3 bucket name

bucket_name = 'bucket'
s3_session = boto3.Session(profile_name='profile')
s3 = s3_session.client('s3')
delta_dir_prefix = 'prefix/'


def list_objects_in_prefix(bucket, prefix):
    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    objects = response.get('Contents', [])
    return objects

def delete_objects_except_latest(bucket, prefix):
    objects = list_objects_in_prefix(bucket, prefix)

    # Sort objects by last modified timestamp in descending order
    sorted_objects = sorted(objects, key=lambda x: x['LastModified'], reverse=True)

    # Keep the latest object and delete the rest
    for obj in sorted_objects[1:]:
        s3.delete_object(Bucket=bucket, Key=obj['Key'])
        print(f"Deleted object: {obj['Key']}")


def main():
    # List all objects in the bucket
    all_objects = list_objects_in_prefix(bucket_name, delta_dir_prefix)

    # Iterate through all objects and their prefixes
    prefixes = set(obj['Key'].split('/')[0] for obj in all_objects)
    for prefix in prefixes:
        delete_objects_except_latest(bucket_name, prefix)

if __name__ == '__main__':
    main()






