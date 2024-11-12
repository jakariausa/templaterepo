import os, json, io, math, sys

from .services.aws import AwsService
from dc_sdk import errors

sys.path.append(os.path.abspath('../src'))

from src.connector import Connector

BUCKET_NAME = f'dataconnector-transfers-local'
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
    def __init__(self, credentials, object_id, filters, fields) -> None:
        self.batch_index = 0
        self.bytes_transferred = 0
        self.filters = filters
        self.object_id = object_id
        self.fields = fields
        self.successful_keys = []

        # Set Pipeline Details
        self.pipeline_details = self._get_pipeline_details()

        self.credentials = credentials
        
        # get connector credentials
        self.connector = Connector(self.credentials)

        self.aws = AwsService(BUCKET_NAME)

        self.row_count = 0

    def authenticate_source(self):
        self.connector.authenticate()
    
    def authenticate_destination(self):
        self.connector.authenticate()

    def get_data(self):
        # Determine batch size

        nrows = self._get_batch_row_count()

        results = self.connector.get_data(self.pipeline_details.source_object_id, self.fields, n_rows=nrows, filters=self.filters)

        while "next_page" in results and results["next_page"] != None:
            if "data" in results and results["data"] != None and results["data"] != []:
                self._process_rows(results["data"])
            elif results["data"] != []:
                print("No rows were fetched")
                # self.internal_log(self.log_templates.INTERNAL_GET_DATA_FETCHED.format(0))
                
            if "next_page" in results and results["next_page"] != None:
                results = self.connector.get_data(self.object_id, self.fields, n_rows=nrows, filters=self.filters, next_page=results["next_page"])
        
        if "data" in results and results["data"] != None and results["data"] != []:
            self._process_rows(results["data"])

# TODO: Implement load data
    # def load_data(self):
    #     self.log(self.log_templates.LOAD_DATA_START.format(self.pipeline_details.destination_object_id, len(self._get_field_ids())))
    #     keys = self.aws.get_keys(self.pipeline_run_history_id)

    #     for index, key in enumerate(keys):
    #         file_object = self.aws.download_object(key)
    #         with io.BytesIO(file_object.get()['Body'].read()) as bio:
    #             data = json.loads(bio)
    #             loaded = self.connector.load_data(data, self.pipeline_details.destination_object_id, self._get_mapping(), self.pipeline_details.update_method_cd, index, len(keys))
    #             if loaded:
    #                 self.row_count = len(data)
    #                 self.update_history({"updateAction": "ROWS_INSERTED", "RowsRetrievedNBR": self.row_count})
    #             else:
    #                 raise errors.LoadDataError("Loading data failed.")
    #     self.log(self.log_templates.LOAD_DATA_FINISHED.format(self.row_count, self.pipeline_details.destination_object_id))

    def _process_rows(self, rows):
        # set row count
        self.row_count += len(rows)
        # set bytes
        self.batch_index += 1
        # convert rows to json
        rows = json.dumps(rows)

        # Upload to s3
        json_buffer = io.StringIO(rows)

        key_name = f"transfers/e-b{self.batch_index}.json"

        self.aws.upload_object(key_name, json_buffer)

        self.successful_keys.append(key_name)
        
        self.bytes_transferred += self.aws.get_object_size(key_name)

    def _get_pipeline_details(self):
        class PipelineDetail:
            def __init__(self, object_id, filters, fields) -> None:
                self.source_object_id = object_id
                self.pipeline_mapping_json = fields
                # self.filtered_column_nm = filters['filtered_column_nm'] if 'filtered_'
                # self.start_selection_nm = filters['start_selection_nm']
                # self.start_value_txt =filters['start_value_txt']
                # self.end_selection_nm = filters['end_selection_nm']
                # self.end_value_txt = filters['end_value_txt']
                # self.timezone_offset_nbr = filters['timezone_offset_nbr']


        return PipelineDetail(self.object_id, self.filters, self.fields)
       
        
    def _get_batch_row_count(self):
        results = self.connector.get_data(self.pipeline_details.source_object_id, self.fields, n_rows=32, filters=self.filters)
        rows = json.dumps(results['data'])

        json_buffer = io.StringIO(rows)

        key_name = f"{TEMP_UPLOADS}/temp.json"

        self.aws.upload_object(key_name, json_buffer)

        file_size = self.aws.get_object_size(key_name)

        self.aws.delete_object(key_name)

        row_size = math.floor(file_size / 32)

        print("32 row byte size: ", file_size)

        print("Row size: ", row_size)

        batch_size = math.floor(5000000 / row_size)

        print("Batch row count: ", batch_size)

        return batch_size

    # def _get_filters(self):
    #     return {
    #         "filtered_column_nm": self.pipeline_details.filtered_column_nm,
    #         "start_selection_nm": self.pipeline_details.start_selection_nm,
    #         "end_selection_nm": self.pipeline_details.end_selection_nm,
    #         "start_value_txt": self.pipeline_details.start_value_txt,
    #         "end_value_txt": self.pipeline_details.end_value_txt,
    #         "timezone_offset_nbr": self.pipeline_details.timezone_offset_nbr
    #     }

    def _get_field_ids(self):
        field_ids = [x["mapped"] for x in json.loads(self.pipeline_details.pipeline_mapping_json)]

        return field_ids

    
    def _get_mapping(self):
        return json.loads(self.pipeline_details.pipeline_mapping_json)