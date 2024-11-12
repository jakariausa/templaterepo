import os, requests, sys, json

from ..models.enums import EnvironmentVariablesEnum
from ..models.pipeline_details import PipelineDetails

try:
    os.environ[EnvironmentVariablesEnum.SERVER_ENDPOINT.value]
    os.environ[EnvironmentVariablesEnum.SERVER_API_KEY.value]
except KeyError as ke:
    print("Missing variable: ", str(ke))
    sys.exit(1)

SERVER_ENDPOINT = os.environ.get(EnvironmentVariablesEnum.SERVER_ENDPOINT.value)
PIPELINE_ID = os.environ.get(EnvironmentVariablesEnum.PIPELINE_ID.value)
SERVER_API_KEY = os.environ.get(EnvironmentVariablesEnum.SERVER_API_KEY.value)
PIPELINES_BASE_URL = f"{SERVER_ENDPOINT}/api/pipelines"

def get(endpoint):
    response = requests.get(f"{PIPELINES_BASE_URL}/{endpoint}", headers={'Accept': 'application/json', 'x-data-connector-access-token': SERVER_API_KEY})

    if response.ok:
        return response.json()
    else:
        print("Error response from server: ", response.json())
        response.raise_for_status()

def post(endpoint, body=None):
    response = requests.post(f"{PIPELINES_BASE_URL}/{endpoint}", data=body, headers={'Accept': 'application/json', 'x-data-connector-access-token': SERVER_API_KEY})

    if response.ok:
        return response.json()
    else:
        print("Error response from server: ", response.json())
        response.raise_for_status()

def put(endpoint, body=None):
    response = requests.put(f"{PIPELINES_BASE_URL}/{endpoint}", data=body, headers={'Accept': 'application/json', 'x-data-connector-access-token': SERVER_API_KEY})

    if response.ok:
        return response.json()
    else:
        print("Error response from server: ", response.json())
        response.raise_for_status()

def log(message, stage, task, error=None, internal=False):
    PIPELINE_RUN_HISTORY_ID = os.environ.get(EnvironmentVariablesEnum.PIPELINE_RUN_HISTORY_ID.value)
    print("Log Output: ", message)
    payload = {
        'PipelineID': PIPELINE_ID,
        'PipelineRunHistoryID': PIPELINE_RUN_HISTORY_ID,
        'LogStageID': stage,
        'LogTXT': message,
        'ExternalFacingFLG': 0 if internal else 1,
        'ErrorLocationDSC': None, # TODO: UPDATE THIS
        'LogTypeCD': 2 if error else 1,
        'LogTypeDSC': 'Error' if error else 'Info',
        'task': task,
    }

    post('log', payload)

def get_pipeline_details(pipeline_id: str, task, pipeline_run_history_id: str):
    json = get(pipeline_id)

    return PipelineDetails(json, task, pipeline_id, pipeline_run_history_id)

def create_new_history(pipeline_id):
    response = post(f"{pipeline_id}/history", None)

    return response['PipelineRunHistoryID']

def send_new_credentials(source_flg, pipeline_id, credentials):
    post(f"credentials/{pipeline_id}", {"source": source_flg, "credentials": json.dumps(credentials)})

def get_credential_updates(source_flg, pipeline_id):
    response = get(f"credentials/{pipeline_id}", {"source": source_flg})

    return response['credential']
    