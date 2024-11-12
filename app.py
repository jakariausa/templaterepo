import os, sys, json
from dotenv import load_dotenv
load_dotenv('.env.credentials')
from conductor.models.enums import RunStagesEnum, TasksEnum, UpdateRunHistoryActionEnum, EnvironmentVariablesEnum
from conductor.services.data_connector_api import create_new_history
try:
    TASK = os.environ[EnvironmentVariablesEnum.TASK.value]
    PIPELINE_ID = os.environ[EnvironmentVariablesEnum.PIPELINE_ID.value]
    if "PIPELINE_RUN_HISTORY_ID" in os.environ:
        PIPELINE_RUN_HISTORY_ID = os.environ[EnvironmentVariablesEnum.PIPELINE_RUN_HISTORY_ID.value]
    else:
        PIPELINE_RUN_HISTORY_ID = create_new_history(PIPELINE_ID)
        os.environ[EnvironmentVariablesEnum.PIPELINE_RUN_HISTORY_ID.value] = str(PIPELINE_RUN_HISTORY_ID)
    APP_ENV = os.environ[EnvironmentVariablesEnum.APP_ENV.value]
    os.environ[EnvironmentVariablesEnum.MASTER_HASH.value]
except KeyError as ke:
    print("Missing variable: ", str(ke))
    sys.exit(1)

import conductor.services.data_connector_api as api
from datetime import datetime, timezone
from conductor.pipeline import PipelineConductor
from dc_sdk import errors

is_source = TasksEnum.SOURCE.value == TASK
print("Initialized Connector task: ", TASK)

def main():
    try:
        pipeline_conductor = PipelineConductor(TASK, pipeline_id=PIPELINE_ID, pipeline_run_history_id=PIPELINE_RUN_HISTORY_ID)
        pipeline_conductor.internal_log(pipeline_conductor.log_templates.INTERNAL_CONNECTOR_START.format(pipeline_conductor.pipeline_details.connector_nm))
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(exc_type, fname, exc_tb.tb_lineno)
        print(e)
        api.log("Fatal error occurred", RunStagesEnum.INITIALIZING_SOURCE_STAGE.value if is_source else RunStagesEnum.INITIALIZING_DESTINATION_STAGE.value, TASK, e, True)
        api.log("An unrecognized issue has occurred on our side. Our team will be in contact within 24-48 hours, or try emailing support@dataconnector.com.", RunStagesEnum.INITIALIZING_SOURCE_STAGE.value if is_source else RunStagesEnum.INITIALIZING_DESTINATION_STAGE.value, TASK, e)
        sys.exit()

    fail = False

    # If job was initialized correctly, start actions    
    try:
        pipeline_conductor.pipeline_details.increment_stage()
        if TASK == TasksEnum.SOURCE.value:
            pipeline_conductor.authenticate_source()
            pipeline_conductor.pipeline_details.increment_stage()
            pipeline_conductor.get_data()
            
            pipeline_conductor.pipeline_details.increment_stage()

            ecs_instance = None

            if APP_ENV != 'local':
                ecs_instance = pipeline_conductor.start_next_connector()
            else:
                raise errors.Error("Next connector not started because connector was ran locally.", "NextNotInvokedError - ", False)

            destination_start = {
                "DestinationECSInstanceID": ecs_instance,
                "updateAction": UpdateRunHistoryActionEnum.DESTINATION_PIPELINE_START.value
            }

            pipeline_conductor.update_history(destination_start)

        else:
            pipeline_conductor.authenticate_destination()
            pipeline_conductor.pipeline_details.increment_stage()
            pipeline_conductor.load_data()

    except errors.Error as e:
        print(type(e))
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(exc_type, fname, exc_tb.tb_lineno)
        fail = True
        if e.internal:
            pipeline_conductor.internal_error(pipeline_conductor.log_templates.INTERNAL_ERROR_MESSAGE.format(type(e).__name__, e.message), e)
        else:
            pipeline_conductor.error(pipeline_conductor.log_templates.ERROR_MESSAGE.format(e.error_name, e.message), e)
    except Exception as ex:
        fail = True
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(exc_type, fname, exc_tb.tb_lineno)

        template = "An exception of type {0} occurred. Arguments:{1!r}"
        message = template.format(type(ex).__name__, ex.args)
        pipeline_conductor.internal_error(message, ex, unhandled=True)

    if is_source:
        end_payload = {
            "StatusCD": 1 if not fail else 3,
            "StatusDSC": "Running" if not fail else "Failed",
            "BytesTransferredNBR": pipeline_conductor.bytes_transferred,
            "RowsRetrievedNBR": pipeline_conductor.row_count,
            "updateAction": UpdateRunHistoryActionEnum.SOURCE_PIPELINE_END.value,
            "credentials": json.dumps(pipeline_conductor.connector.credentials)
        }
    else:
        end_payload = {
            "StatusCD": 2 if not fail else 3,
            "StatusDSC": "Finished" if not fail else "Failed",
            "RowsInsertedNBR": pipeline_conductor.row_count,
            "updateAction": UpdateRunHistoryActionEnum.DESTINATION_PIPELINE_END.value,
            "credentials": json.dumps(pipeline_conductor.connector.credentials)
        }

    pipeline_conductor.update_history(end_payload)

if __name__ == "__main__":
    # TODO: Create new history if environment=local
    try:
        main()
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(exc_type, fname, exc_tb.tb_lineno)
        print(str(e))
        sys.exit(1)