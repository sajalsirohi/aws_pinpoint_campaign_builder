# For more help and to understand the return structure go to 
# https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/pinpoint.html

from ..channel.channel import Channel

class Email(Channel):


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
                       ses_identity_arn, 
                       from_address, 
                       pinpoint_access_role_arn, 
                       enable=True,
                       return_response=False):
        """
        Updates the status of the channel. For first use, enable 
        variable should be true, if you want to disable channel,
        pass false value in the enable variable.

        param: ses_identity_arn : ARN of your email which is approved 
                                  by AWS to send emails. You can find 
                                  it in SES services.

        param: from_address     : The email address approved by AWS, 
                                  present in SES of AWS

        param: pinpoint_access_role_arn : IAM role ARN which has the 
                                  access to pinpoint services

        param: return_response  : False | True, set to true if want 
                                  to return the response from AWS else
                                  nothing will be returned
        """

        response = self.client.update_email_channel(
                ApplicationId=self.application_id,
                EmailChannelRequest={
                    'Enabled': enable,
                    'FromAddress': from_address,
                    'Identity': ses_identity_arn,
                    'RoleArn': pinpoint_access_role_arn
                }
            )
        return response if return_response else ''


    def delete_channel(self, 
                       return_response=True):
        """
        Deletes the channel from the application
        """

        response = self.client.delete_email_channel(
            ApplicationId=self.application_id
        )
        return response if return_response else ''


    def channel_details(self, 
                        return_response=False):
        """
        Returns the details of email channel
        """

        response = self.client.get_email_channel(
            ApplicationId=self.application_id
        )
        return response if return_response else ''


    def create_template(self, 
                        template_name=False,
                        return_response=False,
                        **email_template_request):                        
        """
        Creates a template for the email channel

        param: template_name [REQUIRED]: The name of template. If not 
                                         provided, an error will be raised

        param: **email : {
                            'DefaultSubstitutions': 'string',
                            'HtmlPart': 'string',
                            'RecommenderId': 'string',
                            'Subject': 'string',
                            'tags': {
                                'string': 'string'
                            },
                            'TemplateDescription': 'string',
                            'TextPart': 'string'
                         } 
                         To be provided as it is. For eg.
                         create_template('Template name', HtmlPart=some_string, Subject='Hello from Pinpoint')
        """
        
        assert template_name, 'template_name argument not set. Please provide valid string' 
        response = self.client.create_email_template(
            EmailTemplateRequest=email_template_request,
            TemplateName=template_name
        )
        self.template_name = template_name  
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
            TemplateType='EMAIL'
        )
        return response if return_response else ''


    def set_custom_message(self,
                           body=None,
                           from_address=None,
                           html_body=None,
                           title=None,
                           return_response=False):
        """
        Method to set custom message, if not using templates. If using templates
        then no need to use this method.

        param: body:         The body of the message.

        param: from_address: SES verified email address which will be used to send emails

        param: html_body:    Html version of your body

        param: title:        The subject line, or title, of the email.
        """

        self.custom_email_message = {
            'EmailMessage': {
                    'Body': body,
                    'FromAddress': from_address,
                    'HtmlBody': html_body,
                    'Title': title
                }
        }
        return self.custom_email_message if return_response else ''