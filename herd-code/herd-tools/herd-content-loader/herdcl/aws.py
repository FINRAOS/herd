"""
  Copyright 2015 herd contributors

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
"""
import boto3
from boto3.s3.transfer import S3Transfer
from boto3.exceptions import RetriesExceededError, S3UploadFailedError


class AwsClient:
    def __init__(self, resp, file):
        self.cred = {
            'awsAccessKey': resp.aws_access_key,
            'awsSecretKey': resp.aws_secret_key,
            'awsSessionToken': resp.aws_session_token,
            'bucketName': resp.aws_s3_bucket_name,
            'keyPrefix': resp.s3_key_prefix + file
        }
        self.client = S3Transfer(boto3.client('s3',
                                              region_name='us-east-1',
                                              aws_access_key_id=self.cred['awsAccessKey'],
                                              aws_secret_access_key=self.cred['awsSecretKey'],
                                              aws_session_token=self.cred['awsSessionToken']))

    def get_method(self, method):
        return getattr(self, method)

    def s3_upload(self, path):
        try:
            self.client.upload_file(filename=path,
                                    bucket=self.cred['bucketName'],
                                    key=self.cred['keyPrefix'],
                                    extra_args={'ServerSideEncryption': 'AES256'})
        except S3UploadFailedError as e:
            return e

    def s3_download(self, path):
        try:
            self.client.download_file(bucket=self.cred['bucketName'],
                                      key=self.cred['keyPrefix'],
                                      filename=path)
        except RetriesExceededError as e:
            return e
