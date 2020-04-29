from pinpoint_campaign_builder import PinpointCampaignBuilder

pp = PinpointCampaignBuilder(channel_type=["EMAIL", "SMS"],
                            pinpoint_access_role_arn='arn:aws:iam::392658218916:role/s3-pinpoint-acess',
                            ses_identity_arn='arn:aws:ses:ap-south-1:392658218916:identity/reachus@konfhub.com')

pp.delete_all_apps()