import os
import boto3

APP_ENV = os.environ.get("APP_ENV")

s3_client = boto3.client('s3')
s3_resource = boto3.resource('s3')
ecs_resource = boto3.client('ecs')

class AwsService:
    def __init__(self, s3_bucket) -> None:
        self.s3_bucket = s3_bucket
        self._ecs_cluster_name = "development" if APP_ENV == "development" else "production" if APP_ENV == "production" else "local"

    def get_keys(self, pipeline_run_history_id):
        keys = []
        for key in s3_client.list_objects(Bucket=self.s3_bucket, Prefix=f"transfers/e{pipeline_run_history_id}")['Contents']:
            keys.append(key['Key'])
        return keys

    def get_dev_keys(self, prefix):
        keys = []
        for key in s3_client.list_objects(Bucket=self.s3_bucket, Prefix=f"devTransfers/e{prefix}")['Contents']:
            keys.append(key['Key'])
        return keys

    def download_object(self, key_name):
        return s3_resource.Object(self.s3_bucket, key_name)

    def upload_object(self, key_name, json_buffer):
        s3_resource.Object(
                self.s3_bucket, key_name).put(Body=json_buffer.getvalue())

    def get_object_size(self, key_name):
        response = s3_client.head_object(
            Bucket=self.s3_bucket, Key=key_name)
        return float(response['ContentLength'])

    def delete_object(self, key_name):
        s3_client.delete_object(
            Bucket=self.s3_bucket,
            Key=key_name,
        )


    def start_ecs_task(self, task_name:str, task_version:str, pipeline_id: str, pipeline_run_history_id: str):
        response = ecs_resource.run_task(
            cluster=self._ecs_cluster_name,
            count=1,
            enableECSManagedTags=False,
            enableExecuteCommand=False,
            launchType='FARGATE',
            networkConfiguration={
                'awsvpcConfiguration': {
                    'subnets': [
                        'subnet-095c3230c890e0841',
                    ],
                    'securityGroups': [
                        'sg-00678e3bdcef56894',
                    ],
                    'assignPublicIp': 'ENABLED'
                }
            },
            overrides={
                'containerOverrides': [
                    {
                        'name': task_name,
                        'environment': [
                            {
                                'name': 'PIPELINE_ID',
                                'value': pipeline_id
                            },
                            {
                                'name': 'PIPELINE_RUN_HISTORY_ID',
                                'value': pipeline_run_history_id
                            },
                            {
                                'name': 'TASK',
                                'value': 'DESTINATION'
                            },
                            {
                                'name': 'APP_ENV',
                                'value': os.environ.get("APP_ENV")
                            }
                        ],
                    }
                ],
            },
            platformVersion='LATEST',
            taskDefinition=f"{task_name}:{task_version}"
        )

        if len(response['tasks']) == 0:
            raise Exception("Failed to start next connector")

        print(response)
        print(response['tasks'])

        return response['tasks'][0]['taskArn'].split("/")[-1]