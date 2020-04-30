import json
import boto3
from botocore.errorfactory import ClientError


class s3_utility:


    def __init__(self, BUCKET_NAME, *args):
        
        self.bucket_name = BUCKET_NAME
        self.s3_client   = boto3.client('s3')
        self.s3_resource = boto3.resource('s3')
        
    
    def get_json_file(self, file_name):
        """
        Returns the content of a json file given the file name 
        """
        try:
            s3_clientobj = self.s3_client.get_object(Bucket=self.bucket_name, Key=file_name)
            s3_json_data = json.loads(s3_clientobj['Body'].read().decode('utf-8'))
            return s3_json_data
        except Exception as ex:
            print(ex)
            raise Exception('Custom Exception --\nUnable to read json data from s3, \
                            please check this functionality')

    
    def put_json_to_s3(self, file_name, file_data):
        """
            Helper function to put data to S3
            :param file_name: Name of the file
            :param file_data: Data to be put into the file
        """
        self.s3_client.put_object(Bucket=self.bucket_name,
                                  Key=file_name,
                                  Body=json.dumps(file_data))

    
    def upload_file_to_s3(self, local_file_name, file_name):
        """
            Helper function to upload files into s3
            :param local_file_name: Name of the locally generated file. Eg. details.csv
            :param file_name: File path where file has to be stored
        """
        self.s3_resource.Bucket(self.bucket_name)\
            .upload_file(local_file_name, file_name)


    def download_file(self, s3_file_name, local_file_name):
        """
            Helper function to put data to S3
            :param s3_file_name: Name of the file to be downloaded
            :param local_file_name: Name of the downloaded file which
            can be accessed by '/tmp/{local_file_name} in case of lambdas
        """
        self.s3_client.download_file(self.bucket_name, s3_file_name, local_file_name)
        
    
    def is_file_present(self, file_name):
        """
            Helper function to check if a file_path is present
            in the bucket or not
        """
        try:
            self.s3_client(Bucket=self.bucket_name, Key=file_name)
        except ClientError:
            return False
        return True