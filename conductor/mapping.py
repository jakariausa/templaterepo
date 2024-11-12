from dc_sdk import errors

class Mapping():
    def __init__(self, credentials, Connector):
        self.connector = Connector(credentials)
        
    def connect_get_objects(self):
        results = None
        message = None
        error = None

        try:
            if self.connector.authenticate():
                results = self.connector.get_objects()
                message = "Authenticated successfully."
            else:
                # If not authenticated, raise the authentication error.
                raise errors.AuthenticationError("Failed to authenticate")
        except errors.Error as err:
            message = "{0}{1}".format(err.error_name, err.message)
            pass
        except Exception as ex:
            template = "An exception of type {0} occurred. Arguments:{1!r}"
            message = template.format(type(ex).__name__, ex.args)

        return [results, message, error]
        

    def get_fields(self, object_id, options):
        results = None
        message = None
        error = None

        try:
            if self.connector.authenticate():
                results = self.connector.get_fields(object_id, options=options)
                message = "Retrieved fields."
            else:
                # If not authenticated, raise the authentication error.
                raise errors.AuthenticationError("Failed to authenticate")
        except errors.Error as err:
            message = "{0}{1}".format(err.error_name, err.message)
            pass
        except Exception as ex:
            template = "An exception of type {0} occurred. Arguments:{1!r}"
            message = template.format(type(ex).__name__, ex.args)

        return [results, message, error]

    def get_five_row_preview(self, object_id, field_ids, options):
        results = None
        message = None
        error = None

        try:
            if self.connector.authenticate():
                results = self.connector.get_data(object_id, field_ids=field_ids, n_rows=5, options=options)
                message = "Retrieved 5 row preview."
            else:
                # If not authenticated, raise the authentication error.
                raise errors.AuthenticationError("Failed to authenticate")
        except errors.Error as err:
            message = "{0}{1}".format(err.error_name, err.message)
            pass
        except Exception as ex:
            template = "An exception of type {0} occurred. Arguments:{1!r}"
            message = template.format(type(ex).__name__, ex.args)

        return [results, message, error]
       