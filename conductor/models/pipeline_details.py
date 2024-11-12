from ..models.enums import RunStagesEnum, TasksEnum
import json

class PipelineDetails:
    def __init__(self, row_data, task, pipeline_id, pipeline_run_history_id) -> None:
        self.task = task
        self.pipeline_id = pipeline_id
        self.pipeline_nm = row_data['pipeline_nm']
        self.connector_nm = row_data['source_connector_nm'] if task == "SOURCE" else row_data['destination_connector_nm']
        self.source_object_id = row_data['source_object_id']
        self.destination_object_id = row_data['destination_object_id']
        self.pipeline_mapping_json = row_data['pipeline_mapping_json']
        self.update_method_cd = row_data['update_method_cd']
        self.source_connector_id = row_data['source_connector_id']
        self.source_connector_nm = row_data['source_connector_nm']
        self.destination_connector_id = row_data['destination_connector_id']
        self.destination_connector_nm = row_data['destination_connector_nm']
        self.source_credential_nm = row_data['source_credential_nm']
        self.destination_credential_nm = row_data['destination_credential_nm']
        self.source_encryption_credential_txt = row_data['source_encryption_credential_txt']
        self.destination_encryption_credential_txt = row_data['destination_encryption_credential_txt']
        self.filtered_column_nm = row_data['filtered_column_nm']
        self.start_selection_nm = row_data['start_selection_nm']
        self.start_value_txt =row_data['start_value_txt']
        self.end_selection_nm = row_data['end_selection_nm']
        self.end_value_txt = row_data['end_value_txt']
        self.timezone_offset_nbr = row_data['timezone_offset_nbr']
        self.stage = RunStagesEnum.INITIALIZING_SOURCE_STAGE.value if task == TasksEnum.SOURCE.value else RunStagesEnum.INITIALIZING_DESTINATION_STAGE.value
        self.pipeline_run_history_id = pipeline_run_history_id
        self.destination_ecs_task_nm = row_data['destination_ecs_task_nm']
        self.destination_ecs_task_version_nbr = row_data['destination_ecs_task_version_nbr']
        self.options = json.loads(row_data['pipeline_object_options_json']) if 'pipeline_object_options_json' in row_data else dict()

    def increment_stage(self):
        self.stage += 1