# For more help and to understand the return structure go to 
# https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/pinpoint.html

from ..channel.channel import Channel

class Sms(Channel):


    def __init__(self,
                 client_for_pinpoint,
                 application_id):
        """
        Intializes instace variables with the application_id
        of the project and the client for pinpoint
        """

        self.client = client_for_pinpoint
        self.application_id = application_id
        self.custom_message = None
        self.template_name = None


    def update_channel(self,
                       sender_id='',
                       short_code='', 
                       enable=True,
                       return_response=False):
        """
        Updates the status of the channel. For first use, enable 
        variable should be true, if you want to disable channel,
        pass false value in the enable variable.

        param: sender_id        : The identity that you want to display on
                                  recipients' devices when they receive messages 
                                  from the SMS channel.
                                  To activate it, put a request to AWS support,
                                  after which they will verify the shortcode, and then 
                                  you can use it. Approx 4-8 weeks is required to verify.

        param: short_code       : The registered short code that you want to use when
                                  you send messages through the SMS channel.

        param: return_response  : False | True, set to true if want 
                                  to return the response from AWS else
                                  nothing will be returned

        param: enable           : Set to true, if you want to enable the sms
                                  channel. 
        """

        response = self.client.update_sms_channel(
                ApplicationId=self.application_id,
                SMSChannelRequest={
                    'Enabled': enable,
                    'SenderId': sender_id,
                    'ShortCode': short_code
                }
            )
        return response if return_response else ''


    def delete_channel(self, 
                       return_response=True):
        """
        Deletes the channel from the application
        """

        response = self.client.delete_sms_channel(
            ApplicationId=self.application_id
        )
        return response if return_response else ''


    def channel_details(self, 
                        return_response=True):
        """
        Returns the details of email channel
        """

        response = self.client.get_sms_channel(
            ApplicationId=self.application_id
        )
        return response if return_response else ''


    def create_template(self,
                        template_name=False,
                        return_response=False,
                        **sms_request_template):                        
        """
        Creates a template for the email channel

        param: template_name [REQUIRED]: The name of template. If not 
                                         provided, an error will be raised

        param: **sms : {
                            'Body': 'string', #Only thing important
                            'DefaultSubstitutions': 'string',
                            'RecommenderId': 'string',
                            'tags': {
                                'string': 'string'
                            },
                            'TemplateDescription': 'string'
                        }
                         To be provided as it is. For eg.
                         create_template('Template name', Body=some_string, RecommenderId='Hello from Pinpoint')
        """
        
        assert template_name, 'template_name argument not set. Please provide valid string' 
        response = self.client.create_sms_template(
            SMSTemplateRequest=sms_request_template,
            TemplateName=template_name
        )
        return response if return_response else ''

                                

    def list_template_versions(self, 
                               next_token=None, 
                               page_size=None, 
                               template_name=False,
                               return_response=True):
        """
        Retrieves information about all the versions of a specific message template.

        param: next_token:  The string that specifies which page of results to return
                            in a paginated response. This parameter is not supported
                            for application, campaign, and journey metrics.

        param: page_size:   The maximum number of items to include in each page of a 
                            paginated response. 
        
        param: template_name:  [REQUIRED] Template name for the template
        """
        
        assert template_name, 'template_name argument not set. Please provide valid a string' 
        response = self.client.list_template_versions(
            NextToken=next_token,
            PageSize=page_size,
            TemplateName=template_name,
            TemplateType='SMS'
        )
        return response if return_response else ''


    def set_custom_message(self,
                           body=None,
                           message_type='TRANSACTIONAL',
                           sender_id=None,
                           return_response=False):
        """
        Method to set custom message, if not using templates. If using templates
        then no need to use this method.

        param: body:         The body of the message. [Limit : 160 characters]

        param: message_type: TRANSACTIONAL | PROMOTIONAL

        """

        self.custom_sms_message = {
            'SMSMessage': {
                    'Body': body,
                    'MessageType': message_type,
                    'SenderId': sender_id
                }
        }
        return self.custom_sms_message if return_response else ''
