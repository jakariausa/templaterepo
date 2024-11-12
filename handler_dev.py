import os, re
from src.connector import Connector
from dotenv import load_dotenv

def get_credentials_from_env():
    load_dotenv(".env.credentials")
    # Replace PREF by the prefix you want
    prefix="CRED_"
    myPattern = re.compile(r'{prefix}\w+'.format(prefix=prefix))
    my_env_variables = {key.replace(prefix,''):val for key, val in os.environ.items() if myPattern.match(key)}

    return my_env_variables

def handler(event):
    action = event['action'] if 'action' in event else None
    credentials_dict = event['credentials'] if 'credentials' in event else None
    object_id = event['object_id'] if 'object_id' in event else None
    field_ids = event['mapping'] if 'mapping' in event else None
    message = "Invalid action. Please use 0-3"
    results = None
    error = None
    connector = Connector(credentials_dict)

    try:
        if action == "AUTHENTICATE/GET_OBJECTS":
            connector.authenticate()
            results = connector.get_objects()
            message = "Successfully authenticated and retrieved objects"
        elif action == "GET_FIELDS":
            results = connector.get_fields(object_id)
            message = "Successfully retrieved fields"
        elif action == "GET_FIVE_ROW_PREVIEW":
            results = connector.get_data(object_id, field_ids, 5, None)
            message = "Successfully retrieved 5 row preview"
        elif action == "OAUTH_AUTHENTICATION":
            results = connector.authenticate()
            message = "Successfully got oauth credentials"
        elif action == "YOUR_CUSTOM_TESTS":
            results = None
            message = "Successfully ran custom test..."
        # ADD OTHER TESTS
    except Exception as e:
        error = type(e).__name__
        if hasattr(e, 'message'):
            message = e.message
        else:
            message = f"The error {error} was given."

    print({
        'message': message,
        'results': results,
        'credentials': connector.credentials,
        'error': error
    })

    return {
        'message': message,
        'results': results,
        'credentials': connector.credentials,
        'error': error
    }

if __name__ == "__main__":
    action = "AUTHENTICATE/GET_OBJECTS"
    # action = "OAUTH_AUTHENTICATION"
    # action = "GET_FIELDS"
    # action = "GET_FIVE_ROW_PREVIEW"
    # action = "YOUR_CUSTOM_TESTS"

    credentials = get_credentials_from_env()
    # credentials = {}
    
    object_id = ""
    field_ids = ""

    handler({
        "action": action,
        "credentials": credentials,
        "object_id": object_id,
        "field_ids": field_ids
    })

