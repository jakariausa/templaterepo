class LogTemplates:
    def __init__(self, task=None, json=None) -> None:
        if task == "SOURCE":
            self.AUTHENTICATION_START = "Authenticating through connection {0} to pull data from {1}"# Authenticating through connection: {Connection1} to pull data from {Account}
        else:
            self.AUTHENTICATION_START = "Authenticating through connection: {0} to insert data into {1}"
        
        self.AUTHENTICATION_FINISH = "{0} authenticated successfully"                            # {Connection1} authenticated successfully
        self.GET_DATA_START = "Retrieving the following fields from {0}: {1}"                    # Retrieving the following fields from {Account}: {patientid,source_id,dts}
        self.GET_DATA_FINISH = "Retrieved {0} row(s) from {1}"                                     # Retrieved {4000} rows from {Account}
        self.LOAD_DATA_START = "Loading into {0} for {1} fields"                      # Loading into {dbo.Account} for {29} fields
        self.LOAD_DATA_LOADED = "Inserting {0} out of {1} row(s) into {2}"             # Inserting {1000} out of {4000} row(s) into {dbo.Account}
        self.LOAD_DATA_FINISHED = "Finished loading {0} row(s) into {1}"                         # Finished loading {4000} row(s) into {dbo.Account}
        self.PIPELINE_FINISH = "{0} pipeline finished in {1}"                                     # {Salesforce2SQLServer} pipeline finished in {100.29s}
        
        self.INTERNAL_CONNECTOR_START = "{0} connector started"                                           # {Salesforce} connector started -> Internal
        self.INTERNAL_GET_DATA_FETCHED = "Retrieved {0} row(s)"            # Batch {0} out of {1}: Retrieved {2} row(s)  -> Internal
        self.INTERNAL_START_NEXT_CONNECTOR = "Starting destination connector {0} to load data into {1}"# Starting destination connector {SQL Server} to load data into {dbo.Account} - Internal
        self.INTERNAL_CONNECTOR_END = "Finished {0} connector{1}"                                         # Finished {Salesforce} connector{. Starting next connector} -> Internal? 
        self.INTERNAL_S3_TEMP_UPLOAD_LOG = "S3 Temporary Object {0} was uploaded successfully."
        self.INTERNAL_S3_UPLOAD_LOG = "S3 Object {0} was uploaded successfully."
        self.INTERNAL_ERROR_MESSAGE = "{0} occurred: {1}"
        self.ERROR_MESSAGE = "{0}{1}"
        self.LOAD_DATA_APPEND = "Update method set to append data."
        self.LOAD_DATA_REPLACE = "Update method set to replace data. Clearing {0}." # Update method set to replace data. Clearing {Table, File, other}
        self.LOAD_DATA_CREATE_OBJECT = "{0} does not exist in the {1}. Creating new {0} {2}" # {Table, File, other} does not exist in the {Database, bucket, folder, other}. Creating new {Table, File, other} {dbo.Account}