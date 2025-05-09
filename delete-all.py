import os
import boto3


bucket_name = "data-mining"
s3 = boto3.resource(
    "s3",
    endpoint_url="https://storage.yandexcloud.net",
    aws_access_key_id=os.environ["ACCESS_TOKEN"],
    aws_secret_access_key=os.environ["SECRET_TOKEN"],
)
bucket = s3.Bucket(bucket_name)

print("deleteing all objects")
bucket.object_versions.delete()
print("deleted all objects")

print("deleting all multipart uploads")
for multipart_upload in bucket.multipart_uploads.iterator():
    while len(list(multipart_upload.parts.all())) > 0:
        multipart_upload.abort()

print("done")
