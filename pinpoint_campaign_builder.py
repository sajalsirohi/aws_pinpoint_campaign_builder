"""
A pinpoint campaign builder. Currently supports only EMAIL and SMS channel
"""

import csv
import json
import time
from datetime import datetime
from .email_channel.email_channel import Email
from decimal import Decimal

import boto3
from botocore.exceptions import ClientError

from .s3_utility.s3_utility import s3_utility
from .sms_channel.sms_channel import Sms


class PinpointCampaignBuilder:

    def __init__(self, s3_bucket_name=None,
                 s3_folder_path=None,
                 ses_identity_arn=None,
                 pinpoint_access_role_arn=None,
                 region=None,
                 channel_type=[],
                 base_segment_id=None,
                 application_id=None,
                 application_name=None,
                 email_dynamic_segment_id=None,
                 sms_dynamic_segment_id=None,
                 csv_file_fields=None,
                 email_data=None,
                 sms_data=None,
                 from_address=None,
                 application_exists=False,
                 **additional_args):
        """
            param: s3_bucket_name:      This bucket is used to store the csv file and all project
                                        related files (application_id, segment_ids etc.) If bucket 
                                        name is not provide, then csv file link (used for importing
                                        data into pinpoint) have to be provided (have to be s3 link),
                                        and all the segment_ids to be provided as and when required,
                                        or file link for json containing all the details is required. 
                                        Details required / structure for json file 
                                        {
                                            'base_segment_id': 'string',
                                            'dynamic_email_segment_id: 'string',
                                            'sms_dynamic_id': 'string',
                                            'latest_campaign_launched_at' : 'timestamp'
                                        }

            param: ses_identity_arn :   ARN of your email which is approved by AWS to send emails. 
                                        You can find it in SES services. It is mandatory if you are using
                                        'Email' channel, else not required.

            param: pinpoint_acc_arn :   [REQUIRED] IAM role ARN which has access to all services of pinpoint

            param: region           :   In which region you want to make your pinpoint application. If it is 
                                        not provided, your current region is used.

            param: channel_type     :   List containing the channels your application will use. ["EMAIL","SMS"]
                                        are the only option as of now. 

            param: base_segment_id  :   Segment Id of the base segment of the project.

            param: application_id'  :   If your application is already created, pass the application id, else a new project 
                                        will be created and that project id will be used. If s3_bucket_name is provided,
                                        folder with name of application_id will be created, and all relevant files will be stored
                                        in the folder. for eg csv file which will be imported in pinpoint, project detail file 
                                        containing data as defined in param:s3_bucket_file.

            param: application_name :   Name of the project which will be created in pinpoint. If not provided, current timestamp
                                        will be used as name

            param: email_dynamic_segment_id -> Segement Id for the dynamically created segment which imports EMAIL data from 
                                        the base segment

            param: sms_dynamic_segment_id -> Segement Id for the dynamically created segment which imports SMS data from 
                                        the base segment

            param: csv_file_fields  :   List containing the column names to be written into CSV file, as accepted by AWS.
                                        Please check allowed fields by AWS for imported CSV files. Not setting proper field name
                                        will result in importing error and segment will not be created.
                                        For eg ['ChannelType', 'Address', 'Attributes.Name']

            param: email_data       :   A list containing lists, where each list corresponds to a row entry in CSV file.
                                        eg [['EMAIL', 'sirohisajal@gmail.com', 'sajal']] for the headers defined in csv_file_fields

            param: sms_data         :   A list containing lists, where each list corresponds to a row entry in CSV file. 
                                        eg [['SMS', '+919092XXXXX', 'sajal']] for the headers defined in csv_file_fields.
                                        Provide phone numbers with international codes as defined by AWS.

            param: s3_folder_path   :   Bucket name is required for folder_path. path defines where all the files related to
                                        project will be stored. Default path will be {s3_bucket}/{application_id}

            param: application_exists:  Default False. Set to true, if application exists. It will automatically assign the 
                                        base_segment_id stored in s3_folder_path defined : in s3_bucket_name bucket, and fetches 
                                        the channel type used previously and assign to self.channel_type 
    """
        assert pinpoint_access_role_arn, 'pinpoint_access_role_arn field argument can not be empty'

        self.region_pinpoint = region if region else boto3.session.Session().region_name
        self.client_pinpoint = boto3.client('pinpoint',region_name=self.region_pinpoint)

        self.application_name = application_name if application_name  else str(datetime.now())[:-7]  # keeping name till seconds

        self.application_id = application_id if application_id \
                              else self.create_application(self.application_name)

        self.segment_id_for_campaign = None

        self.s3_folder_path = s3_folder_path if s3_folder_path else f'{self.application_id}'

        self.pinpoint_acc_arn = pinpoint_access_role_arn

        self.channel_type = channel_type

        self.email_data = email_data 

        self.sms_data = sms_data

        self.csv_file_fields = csv_file_fields

        self.base_segment_id = base_segment_id

        self.sms_dynamic_segment_id = sms_dynamic_segment_id

        self.email_dynamic_segment_id = email_dynamic_segment_id

        if s3_bucket_name:
            self.s3_bucket = s3_bucket_name
            self.s3_obj = s3_utility(self.s3_bucket)
        else:
            self.s3_bucket = None
            self.s3_obj = None

        if application_exists:
            if self.s3_bucket:
                self.fetch_pinpoint_data_from_s3()
            if not channel_type:
                self.__get_channels()

        assert self.channel_type, 'channel_type argument can not be empty. Eg. ["EMAIL","SMS"] | ["EMAIL"] | ["SMS"]'

        if 'EMAIL' in channel_type:
            assert ses_identity_arn, 'Please provide ses_identity_role param if you are using EMAIL channel'
            from_address = from_address if from_address else ses_identity_arn.split('/')[-1]
            self.email_obj = Email(self.client_pinpoint, self.application_id)
            if not application_exists:
                self.email_obj.update_channel(ses_identity_arn, from_address, pinpoint_access_role_arn)

        if 'SMS' in channel_type:
            self.sms_obj = Sms(self.client_pinpoint, self.application_id)
            if not application_exists:
                self.sms_obj.update_channel()


    def __get_channels(self):
        """
        If application exists, assigns self.channel_type from the pinpoint application
        """
        response = self.client_pinpoint.get_channels(ApplicationId=self.application_id)
        self.channel_type = [channel for channel in response['ChannelsResponse']['Channels']]
        print(f'Available channel types -> {self.channel_type}, update/activate channels using email or sms objects')


    def create_application(self,
                           application_name,
                           return_full_response=False):
        """
            Create pinpoint application and return the application_id

            param: application_name:    The name of the application that will be created in pinpoint

            param: return_full_response: If set to true, return structure is 
                                        {
                                            'ApplicationResponse': {
                                                'Arn': 'string',
                                                'Id': 'string',
                                                'Name': 'string',
                                                'tags': {
                                                    'string': 'string'
                                                }
                                            }
                                        }, if set to false, returns only application_id

        """
        response = self.client_pinpoint.create_app(
            CreateApplicationRequest={
                'Name': application_name
            }
        )
        return response if return_full_response else response['ApplicationResponse']['Id']


    def delete_application(self,
                           application_id=[]):
        """
        Deletes application given the application ID. If no id is given, uses self.application_id
        """
        if not application_id:
            application_id.append(self.application_id)
    
        [self.client_pinpoint.delete_app(ApplicationId=app_id) for app_id in application_id]


    def delete_all_apps(self):
        """
        Deletes all the pinpoint applications
        """
        response = self.client_pinpoint.get_apps()
        all_application_ids = [item['Id'] for item in response['ApplicationsResponse']['Item']]
        self.delete_application(all_application_ids)


    def get_segments(self):
        """
            Retrieves information about the configuration, dimension, and other settings for all
            the segments that are associated with an application.
            """
        response = self.client_pinpoint.get_segments(
            ApplicationId=self.application_id
        )
        return response


    def __set_data(self,
                   data,
                   csv_file_fields):
        """
            Private method. Assigns data to self.email_data or self.sms_data
            Returns a list containing data to be assigned in
            [['EMAIL', 'sirohisajal@gmail.com', 'sajal']] format.
            
            param: data: A list containing either list or dictionary.
        """
        if isinstance(data[0], dict):
            assert self.csv_file_fields or csv_file_fields, \
                f'Please provide the list of fields in csv_file_fields parameter, with a list of keys used to define ' \
                f'data in data param, else pass data as [[ROW1],[ROW2]]'

            temp_lst = []
            temp_result_list = []

            if not self.csv_file_fields:
                self.csv_file_fields = csv_file_fields

            for _data in data:
                for fields in csv_file_fields:
                    temp_lst.append(_data[fields])
                temp_result_list.append(temp_lst)
                temp_lst = []

        elif isinstance(data[0], list):
            if csv_file_fields:
                self.csv_file_fields = csv_file_fields
            temp_result_list = data

        return temp_result_list


    def set_email_data(self,
                       data,
                       csv_file_fields=None):
        """
            Data of CSV file that will be imported to pinpont.

            param: email_data: [REQUIRED] A list containing lists, or dictionary, where each list or dict corresponds to a
                                row entry in CSV file. eg [['EMAIL', 'sirohisajal@gmail.com', 'sajal']] for the headers
                                defined in csv_file_fields. Dict format is explained in csv_file_fields param.

            param: csv_file_fields: as described in __init__ docs. 
                                    Format of data should be as described by csv_file_fields. 
                                    Else pass data as a list of dictionary with key as csv_file_fields
                                    elements and value being the um well the value.
                                    For eg csv_file_fields = ['ChannelType', 'Address', 'Attributes.Name']
                                    data = [
                                        {
                                            'ChannelType': 'EMAIL',
                                            'Address': 'sirohisajal@gmail.com',
                                            'Attributes.Name': 'sajal sirohi'
                                        },   --> Row 1 data
                                    ]
        """
        assert data, 'Data field can not be Null'
        self.email_data = self.__set_data(data=data, csv_file_fields=csv_file_fields)


    def set_sms_data(self,
                     data,
                     csv_file_fields=None):
        """
            Data of CSV file that will be imported to pinpont.

            param: sms_data: [REQUIRED] A list containing lists, where each list corresponds to a row entry in CSV file. 
                                        eg [['SMS', '+919092XXXXX', 'sajal']] for the headers defined in csv_file_fields.
                                        Provide phone numbers with international codes as defined by AWS.

            param: csv_file_fields: as described in __init__ docs. 
                                    Format of data should be as described by csv_file_fields. 
                                    Else pass data as a list of dictionary with key as csv_file_fields
                                    elements and value being the um well the value.
                                    For eg csv_file_fields = ['ChannelType', 'Address', 'Attributes.Name']
                                    data = [
                                        {
                                            'ChannelType': 'SMS',
                                            'Address': '+91902929292XX',
                                            'Attributes.Name': 'sajal sirohi'
                                        },   --> Row 1 data
                                ]
        """
        assert data, 'Data field can not be Null'
        self.sms_data = self.__set_data(data=data, csv_file_fields=csv_file_fields)


    def set_csv_file_headers(self,
                             csv_file_fields):
        """
            Set fields for the CSV file acc to AWS format
        """
        assert isinstance(csv_file_fields, list), 'Provide csv_file_fields in list format. Eg ["ChannelType"]'
        self.csv_file_fields = csv_file_fields


    def create_dynamic_segment(self,
                               channel,
                               write_segment_request=None,
                               return_full_response=False):
        """
            Creates dynamic segment for the channel type 'channel'. Name of the segment will be
            {channel} dynamic segment.
            Segment will be created with default args, if you want to create segment

            param: channel:     Type of the channel. 'EMAIL' | 'SMS'

            param: write_segment_request : If you want to pass custom values, pass in this variable.
                                           Default values will be used from write_dynamic_segment_request.json file. 
                                           You can do your custom config there. 
                                           In default values, only one source segment is allowed as of now.
        """
        assert channel in ['EMAIL', 'SMS'], 'Channel should be either "SMS" or "EMAIL"'

        if not write_segment_request:
            with open('write_dynamic_segment_request.json') as json_file:
                write_request = json.load(json_file)
                write_request['Dimensions']['Demographic']['Channel']['Values'].append(channel)
                write_request['Name'] = f'{channel} Dynamic Segment'
                write_request['SegmentGroups']['Groups'][0]['Dimensions'][0] \
                    ['Demographic']['Channel']['Values'].append(channel)
                write_request['SegmentGroups']['Groups'][0]['SourceSegments'][0]['Id'] = self.base_segment_id
        else:
            write_request = write_segment_request

        response = self.client_pinpoint.create_segment(
            ApplicationId=self.application_id,
            WriteSegmentRequest=write_request
        )

        if channel == 'EMAIL':
            self.email_dynamic_segment_id = response['SegmentResponse']['Id']
        else:
            self.sms_dynamic_segment_id = response['SegmentResponse']['Id']

        return response if return_full_response else ''


    def import_data_into_pinpoint(self,
                                  csv_file_s3_url=None,
                                  return_full_response=False,
                                  s3_csv_file_path=None,
                                  bucket_name=None,
                                  import_job_request=None,
                                  file_name='pinpoint_details.csv',
                                  update_base_segment=False,
                                  import_segment_name='Base Segment',
                                  **additional_args):
        """
            Import the csv file present either in the bucket (path : s3://bucket_name/{application_id}/{filename}.csv) or
            the direct link provided of the csv file, into pinpoint application as the base segment. It will return 
            the base_segment_id for the project.

            param: csv_file_s3_url:       s3 url of the csv file to be imported

            param: return_full_response:  Default behavior is to return the segment id only

            param: s3_csv_file_path:      File path to the csv file in the self.s3_bucket bucket

            param: bucket_name:           Assign the name of bucket. Then program will assume the path 
                                          {BUCKET_NAME}/{application_id}/{file_name}.csv

            param: file_name:             Name of the CSV file stored in the s3 bucket.

            param: update_base_segment:   Default Value false. It will create a new segment by 
                                          default behavior. If set to True, it will update previously
                                          created segment with the new CSV file data.
        """
        assert self.s3_bucket or bucket_name or csv_file_s3_url, f'Please provide a CSV file url, or a bucket name with file path'

        if not self.s3_bucket and bucket_name:
            self.s3_bucket = bucket_name

        if csv_file_s3_url:
            assert csv_file_s3_url.endswith('.csv') and csv_file_s3_url.startswith('s3://'), \
                f'Format of URL is wrong. URL should start with s3:// and end with .csv. eg s3://XX/details.csv'
            csv_file_url = csv_file_s3_url

        elif self.s3_bucket and s3_csv_file_path:
            assert s3_csv_file_path.endswith('.csv'), \
                f'Given s3 path should end with .csv'
            csv_file_url = f's3://{self.s3_bucket}/{s3_csv_file_path}'

        elif self.s3_bucket:
            csv_file_url = f's3://{self.s3_bucket}/{self.application_id}/{file_name}'

        if not import_job_request:
            import_job_request = {
                'DefineSegment': True,
                'Format': 'CSV',
                'RegisterEndpoints': True,
                'RoleArn': self.pinpoint_acc_arn,
                'S3Url': csv_file_url,
                'SegmentName': import_segment_name
            }
            if update_base_segment:
                assert self.base_segment_id, f'Base_segment_id should be present if you want to update'\
                                f' it, else pass False in update_base_segment param'
                import_job_request['SegmentId'] = self.base_segment_id
                del import_job_request['SegmentName']

        response = self.client_pinpoint.create_import_job(
            ApplicationId=self.application_id,
            ImportJobRequest=import_job_request
        )

        job_id = response['ImportJobResponse']['Id']
        if 'SegmentName' in import_job_request:
            # It is a new segment
            if self.is_segment_imported(job_id):
                response_for_segments = self.client_pinpoint.get_segments(
                    ApplicationId=self.application_id
                )
                self.base_segment_id = response_for_segments['SegmentsResponse']['Item'][0]['Id']
        else:
            while not self.is_segment_imported(job_id):
                print("Waiting for import job to be completed...")


    def is_segment_imported(self,
                            job_id,
                            wait_till=100):
        """
            Returns True if import_job is completed

            param: wait_till:      In seconds, try to import for this much time before raising error
        """
        time_out = 0
        response_import_job = self.client_pinpoint.get_import_job(
            ApplicationId=self.application_id,
            JobId=job_id
        )
        print("Current Status for import job ->", response_import_job['ImportJobResponse']['JobStatus'])
        while response_import_job['ImportJobResponse']['JobStatus'] != 'COMPLETED':
            time_out += 5
            time.sleep(5)
            response_import_job = self.client_pinpoint.get_import_job(
                ApplicationId=self.application_id,
                JobId=job_id
            )
            print("Current Status for import job ->",
                  response_import_job['ImportJobResponse']['JobStatus'])
            if response_import_job['ImportJobResponse']['JobStatus'] == 'FAILED':
                raise Exception("Import Failed, please try again.")

            if time_out >= wait_till:
                print("Time out happened, not able to import file, Abandoning...")
                raise Exception("Import Failed, please try again.")
        return True


    def create_all_segments(self,
                            csv_file_s3_url=None,
                            s3_bucket_name=None,
                            import_segment_name='Base Segment',
                            **additional_args):
        """
            Creates all of the segments for the user. i.e. Base segment [Imported], Email segment [Dynamic], SMS 
            segment [Dynamic]. Dynamic segments are derived from base segment, i.e. to create dynamic segments,
            we must have a imported segment, which is the csv file that will be imported. 

            To import a csv file, bucket_name should be given, or direct S3 link of the csv file should be given.

            param: csv_file_s3_url:  If s3_bucket_name is not given, provide the direct link for the csv file in s3 
                                     bucket.

            param: s3_bucket_name:   In the given bucket, a folder will be created with the name of the application_id
                                     of the pinpoint project, in which the csv file will be uploaded. Then link will be 
                                     dynamically generated and imported into pinpoint automatically.

            param: imported_segment_name: if not given, 'Base Segment' will be used as the imported 
                                     segment name
            """

        assert csv_file_s3_url or s3_bucket_name, 'Please provide either the csv file url or the s3 bucket name'

        if csv_file_s3_url:
            self.import_data_into_pinpoint(csv_file_s3_url=csv_file_s3_url)
        else:
            self.s3_bucket = s3_bucket_name
            self.import_data_into_pinpoint(import_segment_name=import_segment_name)

        self.create_dynamic_segment(channel='EMAIL')
        self.create_dynamic_segment(channel='SMS')


    def create_csv(self,
                   local_csv_file_name='/tmp/pp_details.csv',
                   upload_to_s3=False,
                   csv_file_fields=None,
                   s3_file_path=None,
                   s3_file_name='pinpoint_details.csv',
                   s3_bucket_name=None,
                   **additional_args):
        """
            Create a csv file which will be imported into your pinpoint project. Either create and save the file locally
            or upload the file to s3 automatically by passing the upload_file flag. This will upload the file to the 
            bucket passed in the params. The path will be s3://{bucket_name}/{application_id}/{filename}.csv           

            param: local_csv_file_name  : Name of the csv file generated locally. If not given, value will be /tmp/pp_details.csv

            param: upload_to_s3         : True | False. If true, will be uploaded to s3 using the default file path if 
                                          s3_file_path is not provided.

            param: s3_csv_file_name     : Name of the file when uploaded to s3. If not given,
                                                               value will be pp_details.csv. path -->
                                                               bucket_name/application_id/pp_details.csv

            param: s3_file_path         : Name of whole path instead of using the default path.
                                          Then file will be stored in the specified path.
                                          bucket_name/{some_specific_path}.csv. 
                                          The whole path should be given.
                                          
            param:s3_bucket_name        : bucket name where the file will be stored
        """
        assert self.csv_file_fields or csv_file_fields, 'Please provide csv_file_fields parameter'
        if csv_file_fields:
            self.csv_file_fields = csv_file_fields

        if 'EMAIL' in self.channel_type:
            assert self.email_data, 'Provide email_data using method set_email_data'
            with open(local_csv_file_name, 'w') as csv_file:
                csv_writer = csv.writer(csv_file)
                csv_writer.writerow(self.csv_file_fields)
                csv_writer.writerows(self.email_data)

        if 'SMS' in self.channel_type:
            assert self.sms_data, 'Provide sms_data using the method set_sms_data'
            open_file_as = 'a' if 'EMAIL' in self.channel_type else 'w'
            with open(local_csv_file_name, open_file_as) as csv_file:
                csv_writer = csv.writer(csv_file)
                if open_file_as == 'w':
                    csv_writer.writerow(self.csv_file_fields)
                csv_writer.writerows(self.sms_data)

        if upload_to_s3:
            assert self.s3_bucket or ('s3_bucket_name' in additional_args), 'Please provide a bucket name'
            if 's3_bucket_name' in additional_args:
                self.s3_bucket = additional_args['s3_bucket_name'] 
                self.s3_obj = s3_utility(self.s3_bucket)
            s3_csv_file_name = additional_args['s3_csv_file_name'] if 's3_csv_file_name' in additional_args \
                               else 'pinpoint_details.csv'

            s3_file_path = s3_file_path if s3_file_path else f'{self.application_id}/{s3_csv_file_name}'

            assert s3_file_path.endswith('.csv'), 's3_file_path should end with .csv'

            self.s3_obj.upload_file_to_s3(local_csv_file_name, s3_file_path)         


    def create_campaign(self,
                        campaign_name=None,
                        write_campaign_request=None,
                        schedule_campaign={'StartTime': 'IMMEDIATE'},
                        template_config={},
                        segment_id_for_campaign=None,
                        description=None,
                        return_full_response=False,
                        **additional_args):
        """
        Create pinpoint campaign.

        param: campaign_name        : Name of the campaign. If not given, current time will be used as the
                                      name of the campaign.

        param: write_campaign_request : If you want to use your own custom write campaign request. Else the default
                                      values will be used to launch the campaign.

        param: schedule_campaign    : if you just want to customize the scheduling of the campaign. Default behavior is
                                      campaign will be launched only once, and immediately.

        param: template_config      : If you want to use template, specify the name of the template, with channel as key.
                                      eg {
                                          'SMSTemplate': {
                                              'Name': 'sms_template' [REQUIRED]
                                              'Version': 4           [OPTIONAL] By default, latest version will be used.
                                          }
                                          'EmailTemplate':{..}
                                      }
                                      If you are not using template, then set the custom message for the channel.
                                      eg PinpointCampaignBuilder.email_obj.set_custom_message(body='hi',from_address='sirohisajal@gmail.com')
                                      Same to be done if you are using SMS channel

        param: segment_id_for_campaign : Segment id to be used for campaign. If not provided and if only one channel is provided,
                                      respective dynamic segment id is used, if both channel are used, base_segment_id is used.
        """
        if segment_id_for_campaign:
            self.segment_id_for_campaign = segment_id_for_campaign
        elif 'SMS' in self.channel_type and 'EMAIL' in self.channel_type:
            self.segment_id_for_campaign = self.base_segment_id
        elif 'SMS' in self.channel_type:
            self.segment_id_for_campaign = self.sms_dynamic_segment_id
        elif 'EMAIL' in self.channel_type:
            self.segment_id_for_campaign = self.email_dynamic_segment_id
        else:
            raise Exception('Only EMAIL or SMS or both them allowed in channel_type')

        message_configuration = {}

        if not template_config:
            if 'SMS' in self.channel_type:
                assert self.sms_obj.custom_message, 'Please set the custom message for sms channel using pp.sms_obj.custom_sms_message method'\
                                                        'or use an SMS template.'
                message_configuration.update(self.sms_obj.custom_message)
                
            if 'EMAIL' in self.channel_type:
                assert self.email_obj.custom_message, 'Please set the custom message for sms channel using pp.sms_obj.custom_sms_message method'\
                                                        'or use an EMAIL template.'
                message_configuration.update(self.email_obj.custom_message)

        _write_campaign_request = write_campaign_request if write_campaign_request else {
            'Description': description if description else f'Creating campaign @ {datetime.now()}',
            'IsPaused': False,
            'MessageConfiguration': message_configuration,
            'SegmentId': self.segment_id_for_campaign,
            'Name': campaign_name if campaign_name else f'Campaign @ {str(datetime.now())[:-7]}',
            'Schedule': schedule_campaign,
            'TemplateConfiguration': template_config
        }
     
        response = self.client_pinpoint.create_campaign(
            ApplicationId=self.application_id,
            WriteCampaignRequest=_write_campaign_request
        )
        return response if return_full_response else ''

    
    def send_txn_email(self,
                       sender=None,
                       to_address=None,
                       subject=None,
                       body_text=None,
                       body_html=None,
                       char_set="UTF-8"):
        """
        Send transaction emails from your pinpoint application. Can be used for testing of your email.

        param: sender       : Email id used for sending out emails.
        
        param: to_address   : Receivers address
        
        param: subject      : Subject of the email

        param: body_text    : Text content of the email

        param: body_html    : If receivers client supports html, format your body_text using html 
        """


        try:
            response = self.client_pinpoint.send_messages(
                ApplicationId=self.application_id,
                MessageRequest={
                    'Addresses': {
                        to_address: {
                            'ChannelType': 'EMAIL'
                        }
                    },
                    'MessageConfiguration': {
                        'EmailMessage': {
                            'FromAddress': sender,
                            'SimpleEmail': {
                                'Subject': {
                                    'Charset': char_set,
                                    'Data': subject
                                },
                                'HtmlPart': {
                                    'Charset': char_set,
                                    'Data': body_html
                                },
                                'TextPart': {
                                    'Charset': char_set,
                                    'Data': body_text
                                }
                            }
                        }
                    }
                }
            )
        except ClientError as e:
            print(e.response['Error']['Message'])
        else:
            print("Message sent! Message ID: "
                    + response['MessageResponse']['Result'][to_address]['MessageId'])


    def send_txn_sms(self,
                    origination_number=None,
                    destination_number=None,
                    message="Hello from pinpoint",
                    message_type='TRANSACTIONAL',
                    registered_keyword='',
                    sender_id='',
                    char_set="UTF-8"):
        """
        Send transaction emails from your pinpoint application. Can be used for testing of your email.

        param: origination_number    : The phone number or short code to send the message from. The phone number
                                       or short code that you specify has to be associated with your Amazon Pinpoint
                                       account. For best results, specify long codes in E.164 format
        
        param: destinatio_number     : Receivers number
        
        param: message               : Content of the message

        param: message_type          : TRANSACTIONAL (time sensitive messages) | PROMOTIONAL (marketing related messages)

        param: registered_keyword    : The registered keyword associated with the originating short code.

        param: sender_id             :  The sender ID to use when sending the message. Support for sender ID
                                        varies by country or region. For more information, see
                                        https://docs.aws.amazon.com/pinpoint/latest/userguide/channels-sms-countries.html
        """
        
        try:
            response = self.client_pinpoint.send_messages(
                ApplicationId=self.application_id,
                MessageRequest={
                    'Addresses': {
                        destination_number: {
                            'ChannelType': 'SMS'
                        }
                    },
                    'MessageConfiguration': {
                        'SMSMessage': {
                            'Body': message,
                            'Keyword': registered_keyword,
                            'MessageType': message_type,
                            'OriginationNumber': origination_number,
                            'SenderId': sender_id
                        }
                    }
                }
            )

        except ClientError as e:
            print(e.response['Error']['Message'])
        else:
            print("Message sent! Message ID: "
                    + response['MessageResponse']['Result'][destination_number]['MessageId'])

    
    def get_application_analytics(self):
        """
        Returns analytics for your whole application. Provides all the possible analytics
        """
        kpi_names = ['successful-deliveries-grouped-by-campaign', 'successful-delivery-rate',
                    'email-open-rate', 'unique-deliveries', 'unique-deliveries-grouped-by-date',
                    'successful-delivery-rate-grouped-by-date', 'email-open-rate-grouped-by-campaign']
        response = {}
        {
            response.update({
                kpi_name: self.get_kpi_value(kpi_name)
            }) for kpi_name in kpi_names
        }
            
        return response

    
    def get_kpi_value(self,
                      kpi_name):
        """
        Returns the value of the KPI-name
        """
        kpi_value = 0
        response = self.client_pinpoint.get_application_date_range_kpi(
            ApplicationId=self.application_id,
            KpiName=kpi_name
        )

        response_rows = response['ApplicationDateRangeKpiResponse']['KpiResult']['Rows']
        if response_rows:
            if kpi_name == 'successful-deliveries-grouped-by-campaign':
                kpi_value = sum([self.__get_rounded_value(float(messages_delivered['Values'][0]['Value']))
                                for messages_delivered in response_rows])
            elif kpi_name == 'unique-deliveries-grouped-by-date' or kpi_name == 'successful-delivery-rate-grouped-by-date':
                for data in response_rows:
                    kpi_value = {}
                    value = self.__get_rounded_value(float(data['Values'][0]['Value']))
                    if kpi_name == 'successful-delivery-rate-grouped-by-date':
                        value = value * 100
                    kpi_value.update({data['GroupedBys'][0]['Value']: value})

            elif kpi_name == 'email-open-rate-grouped-by-campaign':
                kpi_value = []
                for data in response_rows:
                    campaign_id = data['GroupedBys'][0]['Value']
                    campaign_name = self.get_campaign_name(campaign_id)
                    kpi_value.append({'Campaign Name': campaign_name,
                                    'Value': self.__get_rounded_value(float(data['Values'][0]['Value']) * 100)})
            else:
                kpi_value = self.__get_rounded_value(float(response_rows[0]['Values'][0]['Value']) * 100)
                if kpi_name == 'unique-deliveries':
                    kpi_value = kpi_value // 100

        return kpi_value


    def get_campaign_name(self,
                          campaign_id):
        """
        Returns the name of campaign
        """
        response = self.client_pinpoint.get_campaign(
            ApplicationId=self.application_id,
            CampaignId=campaign_id
        )
        return response['CampaignResponse']['Name']

    
    def __get_rounded_value(self,
                            data):
        """
        Returns float value up to 2 decimal places
        """
        if len(str(data)) > 5:
            return float(round(Decimal(data), 2))
        return int(round(Decimal(data)))


    def s3_bucket_details(self,
                          s3_bucket=None,
                          s3_folder_path=None,
                          **additional_args):
        """
            Read application details from the s3_bucket. Default path will be {s3_bucket}/{application_id}/application_details.json
            if s3_file_path is not given
        """
        self.s3_bucket = s3_bucket
        self.s3_obj = s3_utility(self.s3_bucket)
        self.s3_folder_path = s3_folder_path if s3_folder_path else f'{self.application_id}'


    def fetch_pinpoint_data_from_s3(self):
        """
            Read pinpoint application details.json file from the s3 bucket, using s3_folder_path if provided
        """
        assert self.s3_bucket, 'Set s3 details using the s3_bucket_details method'
        application_details = self.s3_obj.get_json_file(f'{self.s3_folder_path}/application_details.json')
        self.base_segment_id = application_details['base_segment_id'] if 'base_segment_id' in application_details else None
        self.email_dynamic_segment_id = application_details['email_dynamic_segment_id'] if 'email_dynamic_segment_id' in\
                               application_details else None
        self.sms_dynamic_segment_id = application_details['sms_dynamic_segment_id'] if 'sms_dynamic_segment_id' in\
                               application_details else None     

    
    def update_pinpoint_data_to_s3(self):
        """
            Update/create application_details.json file in the s3 bucket, using s3_folder_path if provided.
        """
        assert self.s3_bucket, 'Set s3 details using s3_bucket_details method'
        data_json = {
            'base_segment_id': self.base_segment_id,
            'email_dynamic_segment_id': self.email_dynamic_segment_id,
            'sms_dynamic_segment_id': self.sms_dynamic_segment_id
        }
        self.s3_obj.upload_json_to_s3(f'{self.s3_folder_path}/application_details.json', data_json)


    def __str__(self):
        """
        Prints all the value of the attributes
        """
        print_statement = f'Application Id    \t\t--> {self.application_id}\n'\
                          f'Application Name    \t\t--> {self.application_name}\n'\
                          f'Base segment Id    \t\t--> {self.base_segment_id}\n'\
                          f'Email dynamic segment Id     --> {self.email_dynamic_segment_id}\n'\
                          f'SMS dynamic segment Id       --> {self.sms_dynamic_segment_id}\n'\
                          f'Channel Types selected       --> {self.channel_type}\n'\
                          f'Bucket Name    \t\t--> {self.s3_bucket}\n'\
                          f'Current Region   \t\t--> {self.region_pinpoint}\n'\
                          f'Segment Id used for campaign --> {self.segment_id_for_campaign}'
        return print_statement