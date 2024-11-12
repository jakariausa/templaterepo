from dotenv import load_dotenv
load_dotenv('.env.credentials')
import json,os
from src.connector import Connector
from flask import Flask, request, Response
from dc_sdk import errors
app = Flask(__name__)
port = os.environ.get("PORT") or 5000
from handler import handler as handler_prod
from conductor.pipeline import PipelineConductor

dc:Connector = None

def unhandled_error(error: Exception):
    print({"message": str(error) if type(error).__name__ != "JSONDecodeError" else "Please pass the body as json.", "error_type": type(error).__name__, "error_arguments": error.args})
    return Response(
        json.dumps({"message": str(error) if type(error).__name__ != "JSONDecodeError" else "Please pass the body as json.", "error_type": type(error).__name__, "error_arguments": error.args}), 
        status=500, 
        mimetype='application/json'
        )

def handled_error(error: errors.Error):
    print({"message": error.message, "error_type": type(error).__name__, "error_arguments": error.args})
    return Response(
        json.dumps({"message": error.message, "error_type": type(error).__name__, "error_arguments": error.args}),
        status=400, 
        mimetype='application/json'
        )

def unhandled_lambda_error(error: Exception, credentials):
    print({"message": str(error) if type(error).__name__ != "JSONDecodeError" else "Please pass the body as json.", "error_type": type(error).__name__, "error_arguments": error.args})
    return {
        "statusCode": 500,
        "headers": {
            "Content-Type": "application/json"
        },
        "body": json.dumps({"message": str(error) if type(error).__name__ != "JSONDecodeError" else "Please pass the body as json.", "credentials": credentials, "error_type": type(error).__name__, "error_arguments": error.args}), 
    }

def handled_lambda_error(error: errors.Error, credentials):
    print({"message": error.message, "error_type": type(error).__name__, "error_arguments": error.args})
    return {
        "statusCode": 400,
        "headers": {
            "Content-Type": "application/json"
        },
        "body": json.dumps({"message": error.message, "credentials": credentials, "error_type": type(error).__name__, "error_arguments": error.args}),
    }

def validate_body(data, required_keys):
    missing_parameters = []
    for key in required_keys:
        if key not in data:
            missing_parameters.append(key)

    if not len(missing_parameters):
        return None

    return Response(
        json.dumps({"message": "Missing one or more required parameters.", "missing_parameters": missing_parameters}),
        status=400, 
        mimetype='application/json'
        )

@app.route('/credentials',methods = ['POST'])
def credentials():
    data = request.data
    json_data = json.loads(data.decode("utf-8"))

    dc.credentials = json_data
    return json_data

@app.route('/authenticate', methods = ['POST'])
def authenticate():
    result = None
    try:
        data = request.data
        json_data = json.loads(data.decode("utf-8"))

        dc = Connector(json_data)

        result = dc.authenticate()
        return Response(json.dumps({"message": "Authenticated Successfully", "results": result}), status=200, mimetype='application/json')
    except errors.Error as er:
        return handled_error(er)
    except Exception as ex:
        return unhandled_error(ex)


@app.route('/get_data', methods= ['POST'])
def get_data():
    result = None

    try:
        request_data = request.data
        json_data: dict = json.loads(request_data.decode("utf-8"))

        object_id = json_data.get("object_id")
        field_ids = json_data.get("field_ids")
        n_rows = json_data.get("n_rows")
        filters = json_data.get("filters")
        next_page = json_data.get("next_page")

        valid = validate_body(json_data, ["object_id", "field_ids", "n_rows"])
        if valid != None:
            return valid

        if not dc.credentials:
            raise Exception("Credentials were not set.")

        result = dc.get_data(object_id, field_ids, int(n_rows), filters, next_page)
        return Response(json.dumps({"message": f"Retrieved {len(result['data'])} row(s) from {object_id}", "results": result}), status=200, mimetype='application/json')
    except errors.Error as er:
        return handled_error(er)
    except Exception as ex:
        return unhandled_error(ex)


@app.route('/load_data', methods= ['POST'])
def load_data():
    result = None

    try:
        request_data = request.data
        json_data: dict = json.loads(request_data.decode("utf-8"))

        object_id = json_data.get("object_id")
        data = json_data.get("data")
        mapping = json_data.get("mapping")
        update_method = json_data.get("update_method")
        index = json_data.get("index")
        total_count = json_data.get("total_count")

        valid = validate_body(json_data, ["object_id", "data", "mapping", "update_method", "index", "total_count"])
        if valid != None:
            return valid

        result = dc.load_data(data, object_id, mapping, update_method, index, total_count)
        return Response(json.dumps({"message": f"Loaded {len(data)} row(s)", "results": result}), status=200, mimetype='application/json')
    except errors.Error as er:
        return handled_error(er)
    except Exception as ex:
        return unhandled_error(ex)

@app.route('/lambda/<event_type>', methods= ['POST'])
def lambda_get_preview(event_type):
    request_data = request.data
    json_data: dict = json.loads(request_data.decode("utf-8"))

    results = handler({"event_type": event_type, **json_data}, None)

    print(results)

    return results

def handler(event, context):
    credentials = event.get("credentials")
    object_id = event.get("object_id")
    field_ids = event.get("field_ids")
    options = event['options'] if 'options' in event else dict()
    mapping = event.get("mapping")
    prefix = event.get("prefix")
    update_method = event.get("update_method")
    filters = event.get("filters")
    message = None
    results = None
    pipeline = None

    try:
        connector = Connector(credentials)

        if event['event_type'] == 'connect':
            if connector.authenticate():
                results = connector.get_objects()
                message = "Authenticated Successfully"
        elif event['event_type'] == 'get_fields':
            if connector.authenticate():
                results = connector.get_fields(object_id, options)
                message = "Retrieved fields"
        elif event['event_type'] == 'get_preview':
            if connector.authenticate():
                results = connector.get_data(object_id, field_ids, 5, filters=filters, options=options)["data"]
                message = "Retrieved 5 preview rows."
        elif event['event_type'] == 'test_source':
            pipeline = PipelineConductor("SOURCE", mode="dev", credentials=credentials, object_id=object_id, mapping=mapping, filters=None, options=None, prefix=prefix)

            pipeline.authenticate_source()
            pipeline.get_data()
            message = "Source Successful."
            results = ", ".join(pipeline.successful_keys)
        elif event['event_type'] == 'test_destination':
            pipeline = PipelineConductor("DESTINATION", mode="dev", credentials=credentials, object_id=object_id, mapping=mapping, prefix=prefix, update_method=update_method)

            pipeline.authenticate_destination()
            pipeline.load_data()
            message = "Destination Successful."
            results = ", ".join(pipeline.successful_keys)
        else:
            raise Exception("event_type was not found.")
    except errors.Error as er:
        return handled_lambda_error(er, pipeline.connector.credentials if pipeline else connector.credentials)
    except Exception as ex:
        return unhandled_lambda_error(ex, pipeline.connector.credentials if pipeline else connector.credentials)
    
    return {
        "statusCode": 200,
        "headers": {
            "Content-Type": "application/json"
        },
        "body": json.dumps({"message": message, "results": results, "credentials": pipeline.connector.credentials if pipeline else connector.credentials}),
    }

@app.route('/lambda-prod', methods= ['POST'])
def lambda_get_preview_prod():

    request_data = request.data
    json_data: dict = json.loads(request_data.decode("utf-8"))

    results = handler_prod(json_data, None)

    print(results)

    return results

if __name__ == "__main__":
    app.run(debug=True, port=port)