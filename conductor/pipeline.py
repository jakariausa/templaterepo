import os, json, io, math, sys
import time
from .services import data_connector_api as api
from .models.log_templates import LogTemplates
from .services.aes_encryption import AesEncryption
from .services.aws import AwsService
from dc_sdk import errors

sys.path.append(os.path.abspath('../src'))

from src.connector import Connector

BUCKET_NAME = f'dataconnector-transfers-{os.environ["APP_ENV"]}'
TEMP_UPLOADS = 'temporary-files'
SUB_FOLDER = 'etlJobHistory'
INITIALIZING_SOURCE_STAGE = 1
INITIALIZING_DESTINATION_STAGE = 2
AUTHENTICATION_SOURCE_STAGE = 3
AUTHENTICATION_DESTINATION_STAGE = 4
RETRIEVING_DATA_STAGE = 5
LOAD_DATA_STAGE = 6
FINISHING_SOURCE_STAGE = 7
FINISHING_DESTINATION_STAGE = 7
DONE = 8


class PipelineConductor:
    def __init__(self, task, mode="prod", **kwargs) -> None:
        self.task = task
        self.mode = mode
        self.aes = AesEncryption()
        self.pipeline_id = kwargs.get("pipeline_id")
        self.pipeline_run_history_id = kwargs.get("pipeline_run_history_id")
        self.batch_index = 0
        self.bytes_transferred = 0
        self.authentication_tries = 0
        self.filters = kwargs.get("filters")
        self.object_id = kwargs.get("object_id")
        self.prefix = kwargs.get("prefix")
        self.update_method = kwargs.get("update_method")
        self.mapping = kwargs.get("mapping")
        self.successful_keys = []

        # Set Pipeline Details
        self.pipeline_details = self._get_pipeline_details() if mode == "prod" else self._get_pipeline_details_dev()

        self.credentials = self._get_credentials() if mode == "prod" else kwargs.get('credentials')
        
        # get connector credentials
        self.connector = Connector(self.credentials)

        self.aws = AwsService(BUCKET_NAME)

        self.row_count = 0
        self.log_templates = self._get_log_messages()

    def authenticate_source(self):
        self.log(self.log_templates.AUTHENTICATION_START.format(self.pipeline_details.source_credential_nm, self.pipeline_details.source_object_id))
        authenticated = False
        while self.authentication_tries <= 3 and not authenticated:
            try:
                authenticated = self.connector.authenticate()
            except Exception as e:
                if self.authentication_tries == 3:
                    raise e
                self.authentication_tries += 1
                encyption_txt = api.get_credential_updates(True, self.pipeline_id)

                self.connector.credentials = self.aes.decrypt(encyption_txt)
                time.sleep(10)

        if authenticated and self.mode == "prod":
            api.send_new_credentials(True, self.pipeline_id, self.connector.credentials)
            self.log(self.log_templates.AUTHENTICATION_FINISH.format(self.pipeline_details.source_credential_nm))
    
    def authenticate_destination(self):
        self.log(self.log_templates.AUTHENTICATION_START.format(self.pipeline_details.destination_credential_nm, self.pipeline_details.destination_object_id))
        self.connector.authenticate()
        self.log(self.log_templates.AUTHENTICATION_FINISH.format(self.pipeline_details.destination_credential_nm))

    def get_data(self):
        # Determine batch size

        self.log(self.log_templates.GET_DATA_START.format(self.pipeline_details.source_object_id, ", ".join(self._get_field_ids())))
        nrows = self._get_batch_row_count()

        results = self.connector.get_data(self.pipeline_details.source_object_id, self._get_field_ids(), n_rows=nrows, filters=self._get_filters(), options=self.pipeline_details.options)

        while "next_page" in results and results["next_page"] != None:
            if "data" in results and results["data"] != None and results["data"] != []:
                self._process_rows(results["data"])
            elif results["data"] != []:
                self.internal_log(self.log_templates.INTERNAL_GET_DATA_FETCHED.format(0))
                
            if "next_page" in results and results["next_page"] != None:
                results = self.connector.get_data(self.pipeline_details.source_object_id, self._get_field_ids(), n_rows=nrows, filters=self._get_filters(), options=self.pipeline_details.options, next_page=results["next_page"])
        
        if "data" in results and results["data"] != None and results["data"] != []:
            self._process_rows(results["data"])
        elif results["data"] != []:
            self.internal_log(self.log_templates.INTERNAL_GET_DATA_FETCHED.format(0))

        self.log(self.log_templates.GET_DATA_FINISH.format(self.row_count, self.pipeline_details.source_object_id))

    def load_data(self):
        self.log(self.log_templates.LOAD_DATA_START.format(self.pipeline_details.destination_object_id, len(self._get_field_ids())))
        keys = self.aws.get_keys(self.pipeline_run_history_id) if self.mode == "prod" else self.aws.get_dev_keys(self.prefix)

        for index, key in enumerate(keys):
            file_object = self.aws.download_object(key)
            with io.BytesIO(file_object.get()['Body'].read()) as bio:
                data = json.load(bio)
                loaded = self.connector.load_data(data, self.pipeline_details.destination_object_id, self._get_mapping(), self.pipeline_details.update_method_cd, index, len(keys))
                if loaded:
                    self.row_count += len(data)
                    if self.mode == "prod":
                        self.update_history({"updateAction": "ROWS_INSERTED", "RowsRetrievedNBR": self.row_count})
                else:
                    raise errors.LoadDataError("Loading data failed.")
        self.log(self.log_templates.LOAD_DATA_FINISHED.format(self.row_count, self.pipeline_details.destination_object_id))

    def start_next_connector(self):
        self.internal_log(self.log_templates.INTERNAL_START_NEXT_CONNECTOR.format(self.pipeline_details.destination_connector_nm, self.pipeline_details.destination_object_id))
        return self.aws.start_ecs_task(self.pipeline_details.destination_ecs_task_nm, str(self.pipeline_details.destination_ecs_task_version_nbr), str(self.pipeline_id), str(self.pipeline_run_history_id))

    def log(self, message):
        if self.mode == "prod":
            api.log(message, self.pipeline_details.stage, self.task)
        else:
            print(message)

    def internal_log(self, message):
        if self.mode == "prod":
            api.log(message, self.pipeline_details.stage, self.task, internal=True)
        else:
            print(message)

    def error(self, message, error: object):
        if self.mode == "prod":
            api.log(message, self.pipeline_details.stage, self.task, error=error)
        else:
            print(message)

    def internal_error(self, message, error: object, unhandled=False):
        if self.mode == "prod":
            api.log(message, self.pipeline_details.stage, self.task, error=error, internal=True)
            if unhandled:
                api.log("An unrecognized issue has occurred on our side. Our team will be in contact within 24-48 hours, or try emailing support@dataconnector.com.", self.pipeline_details.stage, self.task, error)

            api.log(message, self.pipeline_details.stage, self.task, error=error)
        else:
            print(message)
            if unhandled:
                print("An unrecognized issue has occurred on our side. Our team will be in contact within 24-48 hours, or try emailing support@dataconnector.com.")

        
    def update_history(self, payload):
        api.put("{0}/history".format(self.pipeline_run_history_id), payload)

    def _process_rows(self, rows):
        # set row count
        self.row_count += len(rows)
        # set bytes
        self.batch_index += 1
        # convert rows to json
        rows = json.dumps(rows)

        # Upload to s3
        json_buffer = io.StringIO(rows)

        key_name = f"transfers/e{self.pipeline_run_history_id}-b{self.batch_index}.json" if self.mode == 'prod' else f"devTransfers/e{self.prefix}-b{self.batch_index}.json"

        self.aws.upload_object(key_name, json_buffer)

        self.successful_keys.append(key_name)

        self.bytes_transferred += self.aws.get_object_size(key_name)

        self.internal_log(self.log_templates.INTERNAL_GET_DATA_FETCHED.format(self.row_count))
        self.internal_log(self.log_templates.INTERNAL_S3_UPLOAD_LOG.format(key_name))

        if self.mode == 'prod':
            self.update_history({"updateAction": "ROWS_RETRIEVED", "RowsRetrievedNBR": self.row_count})

    def _get_credentials(self):
        encyption_txt = self.pipeline_details.source_encryption_credential_txt if self.task == "SOURCE" else self.pipeline_details.destination_encryption_credential_txt

        # Set Connector Credentials
        return self.aes.decrypt(encyption_txt)

    def _get_pipeline_details(self):
        return api.get_pipeline_details(str(self.pipeline_id), self.task, str(self.pipeline_run_history_id))
        
    def _get_pipeline_details_dev(self):
        class PipelineDetail:
            def __init__(self, object_id, filters, mapping, update_method) -> None:
                self.source_object_id = object_id
                self.destination_object_id = object_id
                self.pipeline_mapping_json = mapping
                self.update_method_cd = update_method
                self.source_credential_nm = "Test"
                self.destination_credential_nm = "Test"
                # TODO: UPDATE OPTIONS
                self.options = None
                # self.filtered_column_nm = filters['filtered_column_nm'] if 'filtered_'
                # self.start_selection_nm = filters['start_selection_nm']
                # self.start_value_txt =filters['start_value_txt']
                # self.end_selection_nm = filters['end_selection_nm']
                # self.end_value_txt = filters['end_value_txt']
                # self.timezone_offset_nbr = filters['timezone_offset_nbr']

        return PipelineDetail(self.object_id, self.filters, self.mapping, self.update_method)

    def _get_batch_row_count(self):
        results = self.connector.get_data(self.pipeline_details.source_object_id, self._get_field_ids(), n_rows=32, filters=self._get_filters())
        rows = json.dumps(results['data'])

        json_buffer = io.StringIO(rows)

        key_name = f"{TEMP_UPLOADS}/{self.pipeline_details.pipeline_run_history_id}.json" if self.mode == "prod" else f"devTransfers/{self.prefix}.json"

        self.aws.upload_object(key_name, json_buffer)

        self.internal_log(self.log_templates.INTERNAL_S3_UPLOAD_LOG.format(key_name))

        file_size = self.aws.get_object_size(key_name)

        self.aws.delete_object(key_name)

        row_size = math.floor(file_size / 32)

        print("32 row byte size: ", file_size)

        print("Row size: ", row_size)

        batch_size = math.floor(5000000 / row_size)

        print("Batch row count: ", batch_size)

        return batch_size

    def _get_filters(self):
        if self.mode == "prod":
            return {
                "filtered_column_nm": self.pipeline_details.filtered_column_nm,
                "start_selection_nm": self.pipeline_details.start_selection_nm,
                "end_selection_nm": self.pipeline_details.end_selection_nm,
                "start_value_txt": self.pipeline_details.start_value_txt,
                "end_value_txt": self.pipeline_details.end_value_txt,
                "timezone_offset_nbr": self.pipeline_details.timezone_offset_nbr
            }
        else: 
            return self.filters

    def _get_field_ids(self):
        field_ids = [x["mapped"] for x in json.loads(self.pipeline_details.pipeline_mapping_json)]

        return field_ids

    
    def _get_mapping(self):
        return json.loads(self.pipeline_details.pipeline_mapping_json) if self.mode == "prod" else json.loads(self.mapping)

    def _get_log_messages(self):
        # TODO: Make logs more dynamic
        # logs = api.get("log/templates")
        return LogTemplates(task=self.task)