import os
import boto3

s3_client = boto3.client('s3')

def get_matching_s3_objects(bucket, prefix='', suffix=''):
    """
    Generate objects in an S3 bucket.
    :param bucket: Name of the S3 bucket.
    :param prefix: Only fetch objects whose key starts with
        this prefix (optional).
    :param suffix: Only fetch objects whose keys end with
        this suffix (optional).
    """
    kwargs = {'Bucket': bucket}

    # If the prefix is a single string (not a tuple of strings), we can
    # do the filtering directly in the S3 API.
    if isinstance(prefix, str):
        kwargs['Prefix'] = prefix

    while True:

        # The S3 API response is a large blob of metadata.
        # 'Contents' contains information about the listed objects.
        resp = s3_client.list_objects_v2(**kwargs)

        try:
            contents = resp['Contents']
        except KeyError:
            return

        for obj in contents:
            key = obj['Key']
            if key.startswith(prefix) and key.endswith(suffix):
                yield obj

        # The S3 API is paginated, returning up to 1000 keys at a time.
        # Pass the continuation token into the next response, until we
        # reach the final page (when this field is missing).
        try:
            kwargs['ContinuationToken'] = resp['NextContinuationToken']
        except KeyError:
            break


def get_matching_s3_keys(bucket, prefix='', suffix=''):
    """
    Generate the keys in an S3 bucket.
    :param bucket: Name of the S3 bucket.
    :param prefix: Only fetch keys that start with this prefix (optional).
    :param suffix: Only fetch keys that end with this suffix (optional).
    """
    for obj in get_matching_s3_objects(bucket, prefix, suffix):
        yield obj['Key']

class S3_Manager():
    def __init__(self):
        self.bucket_name = os.environ["s3_bucket"]

    def create_s3_directory(self, directory_name, dir_path="/"):
        try:
            print("Creating directory {} in s3 {}".format(directory_name, self.bucket_name))
            s3_client.put_object(Bucket=self.bucket_name, Key=(directory_name + dir_path))
        except Exception as error:
            print("Error occured in creating s3 directory {} due to {}".format(directory_name, error))

    def rename_s3_directory_file(self, old_dir_name, new_dir_name, dir_path="/"):
        try:
            # Create a new directory
            s3_client.put_object(Bucket=self.bucket_name, Key=(new_dir_name + dir_path))

            files = get_matching_s3_keys(bucket = self.bucket_name, prefix=old_dir_name + dir_path)
            for each in files:
                new_file = each.replace(old_dir_name + dir_path, new_dir_name + dir_path)
                print("Copying directory/file {}".format(new_file))

                copy_source = {'Bucket': self.bucket_name, 'Key': each}
                s3_client.copy_object(CopySource = copy_source, Bucket = self.bucket_name, Key = new_file)

                print("Deleting directory/file {}".format(old_dir_name + dir_path))
                s3_client.delete_object(Bucket=self.bucket_name, Key=each)

        except Exception as error:
            print("Error occured while renaming directory/file from {} to {} due to {}".format(old_dir_name, new_dir_name, error))

    def delete_s3_directory_file(self, dir_name, dir_path="/"):
        try:
            files = get_matching_s3_keys(bucket = self.bucket_name, prefix=dir_name + dir_path)
            for each in files:
                print("Deleting directory/file {}".format(dir_name + dir_path))
                s3_client.delete_object(Bucket=self.bucket_name, Key=each)

        except Exception as error:
            print("Error occured while deleting directory from {} due to {}".format(dir_name, error))

    def save_s3_file(self, file_name, content):
        print(file_name, type(content))
        try:
            response = s3_client.upload_fileobj(content, self.bucket_name, file_name)
            print(response)
        except Exception as error:
            print("Error occured while writing contents to file {} due to {}".format(file_name, error))
