# region imports
from dc_sdk import errors
from mysql.connector import Error
from mysql.connector import errorcode
from psycopg2 import OperationalError
from psycopg2 import extras
from psycopg2 import sql
from psycopg2.extras import execute_batch
import logging
import mysql.connector
import psycopg2
# end region imports

# region class
class Connector:
    # region init
    def __init__(self, credentials):
        """
        store the credentials
        :param credentials: the credentials needed to authenticate with the connector, formatted as a dictionary
        """
        self.credentials = credentials
        self.batch_size = None
    # end region init
    # region authenticate
    class AuthenticationError(Exception):
        """Custom exception for authentication errors."""
        pass
    
    def authenticate(self):
        """Establish and validate a connection to a PostgreSQL database."""
        try:
            # Extract credentials from the instance's attributes
            username = self.credentials.get('username')
            password = self.credentials.get('password')
            server = self.credentials.get('server')
            database = self.credentials.get('database')
            
            # Establish the connection
            self.connection = psycopg2.connect(
                user=username,
                password=password,
                host=server,
                database=database,
                connect_timeout=50  # Set the timeout to 50 seconds
            )
            
            # Log successful connection
            logging.info("Connection established successfully.")
            return True
    
        except OperationalError as e:
            # Log the error and raise a custom AuthenticationError
            logging.error("Authentication failed: %s", e)
            raise AuthenticationError(f"Authentication failed: {e}")
    # end region authenticate
    # region get_metadata
    def get_metadata(self):
        """
        get metadata relating to the object. This is just for destination connectors
        :return: a dictionary containing metadata relating to the object, formatted as
                {
                  "column_type_flg": true | false,
                  "size_flg": true | false,
                  "new_object_regex": "string" | null,
                  "size_regex": "string" | null,
                  "data_types": ["string1", "string2"] | null,
                  "object_id_delimiter": "string" | null  # used to separate the group from the id of the object
                }
        """
        raise errors.NotImplementedError()
    # end region get_metadata
    # region get_objects
    def get_objects(self):
        """
        returns a list of all objects connected to the user's account,
            where object refers to the container of data, whether that is a table, spreadsheet, Salesforce object, etc.
        :return: a list of dictionaries, where each dictionary contains information about the object
            [
                {
                    object_id: <object_id>,
                    object_name: <object_name>,
                    object_label: <object_label>,
                    object_group: <object_group>
                },
                {...},
                ...
            ]
            object_id: the unique identifier of the object. It's what the connector uses to access the object.
            object_name: the actual name of the object. if the object_name is None,
                the frontend will only display the object_label
            object_label: what should be displayed by the frontend.
        """
        raise errors.NotImplementedError()
    # end region get_objects
    # region get_fields
    def get_fields(self, object_id, options=dict()):
        """
        returns a list of all fields (columns) connected to the specified object (table)
        :param object_id: one of the object_id's returned by the get_objects() function
        :return: a list of dictionaries, where each dictionary contains information about a field
        [
            {
                field_id: <field_id>,
                field_name: <field_name>,
                field_label: <field_label>,
                data_type: <data_type>,
                size: <size>
            },
            {...},
            ...
        ]
        """
        raise errors.NotImplementedError()
    # end region get_fields
    # region determine_batch_size
    def determine_batch_size(self, object_id, field_ids, filters=None):
        """
        determines how many rows can be pulled at once without going over the 5mB limit. This is done automatically by
        some APIs, so it might not need to be implemented
        :param object_id:
        :param field_ids:
        :param filters:
        :return:
        """
        self.batch_size = 1000
        raise errors.NotImplementedError()
    # end region determine_batch_size
    # region get_data
    def get_data(self, object_id, field_ids, n_rows=None, filters=None, next_page=None, options=dict()):
        """
        :param object_id: one of the object_id's returned by the get_objects() function
        :param field_ids: a list of strings containing the field_ids returned by the get_fields() function
        :param n_rows: the number of rows for which to return the data
        :param filters:
        {
            filtered_column_nm: <column name>,
            start_selection_nm: <category of start filter ("Today", "Yesterday", "Today Subtract", "Custom Date")>,
            end_selection_nm: <category of end filter ("Today", "Yesterday", "Today Subtract", "Custom Date")>,
            start_value_txt: <the date of the beginning of the range by which to filter>,
            end_value_txt: <the date of the end of the range by which to filter>,
            timezone_offset_nbr: <integer representing offset from UTC time>
        }
                if start_selection_nm is None, pull all data from the beginning of time to the end date
                if end_selection_nm is None, pull all data from the start date to the most recent stuff
        :param next_page: This gives the identifier for the next batch to be pulled if the data is too large to pull all
        together. For some connectors, this may be an identifier returned by the API, and for others, this may need to
        be an identifier for the next rows to pull.
        :return: the data pulled from the source, formatted as an list of dicts where each dict is a row
            {next_page: <the identifier for the next page to pull or None>,
            data:
            [{field_ids[1]: <value string in 1st row, 1st column>,
              field_ids[2]: <value string in 1st row, 2nd column>, ...},
            {field_ids[1]: <value string in 2nd row, 1st column>,
             field_ids[2]: <value string in 2nd row, 2nd column>, ...},
            ...
            {field_ids[N]: <value string in Nth row, 1st column>, ...}]
            }
        """
        raise errors.NotImplementedError()
    # end region get_data
    # region load_data
    # Necessary imports for the MySQL database connector
    
    # Inside your existing class, place the following methods
    
    # region load_data
    
    def load_data(self, data, object_id, m, update_method, batch_number, total_batches, credentials):
        """
        Load data into the specified database table (object_id).
        """
        connection = None
        cursor = None
        
        try:
            # Establish the connection using credentials
            connection = mysql.connector.connect(
                host=credentials['host'],
                database=credentials['dbname'],
                user=credentials['user'],
                password=credentials['password'],
                port=credentials.get('port', 3306)
            )
            cursor = connection.cursor()
    
            # Log connection establishment
            print(f"Connection established to database: {credentials['dbname']}")
    
            # Check and create table if it doesn't exist, only for the first batch
            if batch_number == 0:
                self.create_table_if_not_exists(cursor, object_id, m)
    
            # Transform data into tuples based on the mapping
            transformed_data = self.transform_data(data, m)
    
            # Perform the data loading operation based on update_method
            if update_method == 0:
                self.append_data(cursor, object_id, transformed_data, m)
            elif update_method == 1:
                self.replace_data(cursor, object_id, transformed_data, m)
            elif update_method == 2:
                raise NotImplementedError("Upsert operation is not supported for MySQL.")
            else:
                raise ValueError("Invalid update method specified.")
    
            # Commit the transaction
            connection.commit()
            print(f"Data loaded successfully into {object_id} using method {update_method}")
    
        except Error as e:
            if connection:
                connection.rollback()
            print(f"An error occurred: {str(e)}")
            raise
    
        finally:
            # Ensure the cursor and connection are closed
            if cursor:
                cursor.close()
            if connection:
                connection.close()
            print("Connection closed.")
    
    def create_table_if_not_exists(self, cursor, object_id, m):
        """
        Create the table if it does not exist, using the mapping to define columns.
        """
        columns = []
        for mapping in m:
            col_def = f"{mapping['destination_field_id']} {mapping['datatype']}"
            if mapping['datatype'].upper() == 'VARCHAR' and mapping.get('size'):
                col_def += f"({mapping['size']})"
            columns.append(col_def)
        columns_def = ", ".join(columns)
        create_table_query = f"CREATE TABLE IF NOT EXISTS {object_id} ({columns_def})"
        cursor.execute(create_table_query)
        print(f"Table {object_id} checked/created.")
    
    def transform_data(self, data, m):
        """
        Transform the list of dictionaries into a list of tuples based on the column mapping.
        """
        transformed_data = []
        for row in data:
            row_tuple = tuple(row[mapping['source_field_id']] for mapping in m)
            transformed_data.append(row_tuple)
        return transformed_data
    
    def append_data(self, cursor, object_id, transformed_data, m):
        """
        Append data to the table using batch inserts.
        """
        insert_query = self.generate_insert_query(object_id, m)
        cursor.executemany(insert_query, transformed_data)
        print(f"Data appended to {object_id}.")
    
    def replace_data(self, cursor, object_id, transformed_data, m):
        """
        Replace data in the table by deleting existing data and inserting new data.
        """
        cursor.execute(f"DELETE FROM {object_id}")
        print(f"Existing data in {object_id} deleted.")
        self.append_data(cursor, object_id, transformed_data, m)
    
    def generate_insert_query(self, object_id, m):
        """
        Generate an insert query dynamically based on the provided mapping.
        """
        columns = ", ".join(mapping['destination_field_id'] for mapping in m)
        placeholders = ", ".join(["%s"] * len(m))
        insert_query = f"INSERT INTO {object_id} ({columns}) VALUES ({placeholders})"
        return insert_query
    
    # endregion load_data
    # end region load_data
    # region other_functions
    # end region other_functions
#end region class
