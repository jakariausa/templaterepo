import requests, os, sys
import boto3
from dotenv import load_dotenv
load_dotenv('.env.credentials')

server_endpoint = os.environ.get("SERVER_ENDPOINT")
PIPELINES_BASE_URL = f"{server_endpoint}/api/pipelines"
ecs_client = boto3.client('ecs')

def post_request(endpoint, body):
    response = requests.post(f"{PIPELINES_BASE_URL}/{endpoint}", data=body, headers={'Accept': 'application/json', 'x-data-connector-access-token': os.environ.get("SERVER_API_KEY")})

    if response.ok:
        return response.json()
    else:
        print("Error response from server: ", response.json())
        response.raise_for_status()

def put_request(endpoint, body):
    response = requests.put(f"{PIPELINES_BASE_URL}/{endpoint}", data=body, headers={'Accept': 'application/json', 'x-data-connector-access-token': os.environ.get("SERVER_API_KEY")})

    if response.ok:
        return response.json()
    else:
        print("Error response from server: ", response.json())
        response.raise_for_status()

def create_new_history(pipeline_id) -> int:
    response = post_request(f"{pipeline_id}/history", None)

    print("New history response: ", response)

    return response['PipelineRunHistoryID']

def update_history_instance_arn(pipeline_run_history_id, instance_arn):
    response = put_request(f"{pipeline_run_history_id}/history", {"SourceECSInstanceID": instance_arn, "updateAction": "SOURCE_ECS_ID"})

    print(response)

def start_ecs_task(cluster, container_nm, pipeline_run_history_id, pipeline_id, version_nbr):
    response = ecs_client.run_task(
        cluster=cluster,
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
                    'name': container_nm,
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
                            'value': 'SOURCE'
                        }
                    ],
                }
            ],
        },
        platformVersion='LATEST',
        taskDefinition=f"{container_nm}:{version_nbr}"
    )

    arn = response['tasks'][0]['containers'][0]['taskArn']

    return arn.split("/")[-1]

def handler(event, _):
    # create new job history
    # invoke ecs pipeline


    pipeline_run_history_id = None
    pipeline_id = event['pipeline_id']

    pipeline_run_history_id = create_new_history(pipeline_id)

    print(pipeline_id, pipeline_run_history_id)

    os.environ["PIPELINE_RUN_HISTORY_ID"] = pipeline_run_history_id

    

if __name__ == "__main__":
    pipeline_id = os.environ["PIPELINE_ID"]

    handler({"pipeline_id": pipeline_id}, None)

    from app import main
    try:
        main()
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(exc_type, fname, exc_tb.tb_lineno)
        print(str(e))
        sys.exit(1)

    # instance_arn = start_ecs_task("dc-3", "python3-salesforce", str(pipeline_run_history_id), str(pipeline_id), "9")

    # update_history_instance_arn(pipeline_run_history_id, instance_arn)
