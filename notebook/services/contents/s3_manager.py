import boto3
s3_client = boto3.client('s3')

class S3_Manager():
    def __init__(self):
        self.bucket_name = "fsot-cloudfront-test-bucket"

    def create_s3_directory(self, directory_name, dir_path="/"):
        try:
            print("Creating directory {} in s3 {}".format(directory_name, self.bucket_name))
            s3_client.put_object(Bucket=self.bucket_name, Key=(directory_name + dir_path))
        except Exception as error:
            print("Error occured in creating s3 directory {} due to {}".format(directory_name, error))

    def rename_s3_directory_file(self, old_dir_name, new_dir_name, dir_path="/"):
        try:
            # Copy object A as object B
            copy_source = {'Bucket': self.bucket_name, 'Key': old_dir_name + dir_path}
            s3_client.copy_object(Bucket=self.bucket_name, CopySource=copy_source, Key=new_dir_name + dir_path)
            print("Copying old directory/file {} to new directory/file {}".format(old_dir_name + dir_path, new_dir_name + dir_path))

            # Delete the former object A
            s3_client.delete_object(Bucket=self.bucket_name, Key=old_dir_name + dir_path)
            print("Deleting old directory/file {}".format(old_dir_name + dir_path))
        except Exception as error:
            print("Error occured while renaming directory/file from {} to {} due to {}".format(old_dir_name, new_dir_name, error))

    def delete_s3_directory_file(self, dir_name, dir_path="/"):
        try:
            s3_client.delete_object(Bucket=self.bucket_name, Key=dir_name + dir_path)
            print("Deleting directory/file {}".format(dir_name + dir_path))
        except Exception as error:
            print("Error occured while deleting directory from {} due to {}".format(dir_name, error))

    def save_s3_file(self, file_name, content):
        print(file_name, type(content))
        try:
            response = s3_client.upload_fileobj(content, self.bucket_name, file_name)
            print(response)
        except Exception as error:
            print("Error occured while writing contents to file {} due to {}".format(file_name, error))
