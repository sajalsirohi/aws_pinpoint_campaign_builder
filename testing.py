from pinpoint_campaign_builder import PinpointCampaignBuilder

pp = PinpointCampaignBuilder(channel_type=["EMAIL", "SMS"],
                            pinpoint_access_role_arn='arn:aws:iam::{some_id}:role/s3-pinpoint-acess',
                            ses_identity_arn='arn:aws:ses:{your-region}:{some_id}:identity/{email_id}')

pp.delete_all_apps()
