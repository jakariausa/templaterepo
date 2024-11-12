from conductor.mapping import Mapping
from src.connector import Connector

def handler(event, context):
    """Lambda Handler"""

    action = int(event['action'])
    credentials_dict = event['credentials'] if 'credentials' in event else None
    object_id = event['object_id'] if 'object_id' in event else None
    field_ids = event['mapping'] if 'mapping' in event else None
    options = event['options'] if 'options' in event else dict()
    message = "Invalid action. Please use 0-3"
    results = None
    error = None
    mapping = Mapping(credentials_dict, Connector)

    try:
        if action == 0:
            results, message, error = mapping.connect_get_objects()
        elif action == 1:
            results, message, error = mapping.get_fields(object_id, options)
        elif action == 2:
            results, message, error = mapping.get_five_row_preview(
                object_id, field_ids, options)
    except Exception as e:
        print(e)
        message = f"Unhandled error: {str(e)}"

    return {
        'message': message,
        'results': results,
        'credentials': mapping.connector.credentials,
        'error': error
    }
