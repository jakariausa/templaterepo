# region imports
from dc_sdk import errors
from psycopg2 import extras
from psycopg2 import sql
from psycopg2.extras import execute_batch
import logging
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
    def authenticate(self):
        """
        authenticate with the connector using self.credentials, update self.credentials if credentials change
        :return: a boolean indicating whether the connector was able to successfully authenticate
        """
        raise errors.NotImplementedError()
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
    # Necessary imports for PostgreSQL connection and operations
    
    # Assuming these methods will be placed inside an existing class
    # The `self` parameter is used to align with class method definitions
    
    # region load_data
    
    def load_data(self, data, object_id, m, update_method, batch_number, total_batches, credentials):
        connection = None
        cursor = None
        try:
            # Establishing the connection to the database
            connection = psycopg2.connect(
                dbname=credentials['dbname'],
                user=credentials['user'],
                password=credentials['password'],
                host=credentials['host'],
                port=credentials['port']
            )
            cursor = connection.cursor()
            self.log("Connection established successfully.")
    
            # Transform data into list of tuples
            data_tuples = [
                tuple(row[mapping['source_field_id']] for mapping in m)
                for row in data
            ]
    
            # Create table if it is the first batch
            if batch_number == 0:
                self.create_table_if_not_exists(cursor, object_id, m)
                connection.commit()
                self.log(f"Table '{object_id}' verified/created.")
    
            # Execute the appropriate update method
            if update_method == 0:
                self.append_data(cursor, object_id, m, data_tuples)
            elif update_method == 1:
                self.replace_data(cursor, object_id, m, data_tuples)
            elif update_method == 2:
                raise NotImplementedError("Upsert operation is not supported.")
            else:
                raise ValueError("Invalid update method specified.")
    
            # Commit the transaction
            connection.commit()
            self.log("Data loaded successfully.")
    
        except Exception as e:
            if connection:
                connection.rollback()
            self.log(f"Error occurred: {e}")
            raise
        finally:
            if cursor:
                cursor.close()
            if connection:
                connection.close()
            self.log("Connection closed.")
    
    def create_table_if_not_exists(self, cursor, object_id, m):
        columns = [
            f"{mapping['destination_field_id']} {self.get_sql_datatype(mapping['datatype'], mapping.get('size'))}"
            for mapping in m
        ]
        create_table_query = sql.SQL(
            "CREATE TABLE IF NOT EXISTS {table} ({fields})"
        ).format(
            table=sql.Identifier(object_id),
            fields=sql.SQL(', ').join(sql.SQL(column) for column in columns)
        )
        cursor.execute(create_table_query)
        self.log(f"Create table query executed: {create_table_query.as_string(cursor)}")
    
    def get_sql_datatype(self, datatype, size=None):
        if datatype.upper() == "VARCHAR" and size:
            return f"VARCHAR({size})"
        elif datatype.upper() in ["INT", "INTEGER"]:
            return "INTEGER"
        else:
            return datatype.upper()
    
    def generate_insert_query(self, object_id, m):
        columns = [mapping['destination_field_id'] for mapping in m]
        placeholders = [sql.Placeholder() for _ in columns]
        insert_query = sql.SQL(
            "INSERT INTO {table} ({fields}) VALUES ({values})"
        ).format(
            table=sql.Identifier(object_id),
            fields=sql.SQL(', ').join(map(sql.Identifier, columns)),
            values=sql.SQL(', ').join(placeholders)
        )
        return insert_query
    
    def append_data(self, cursor, object_id, m, data_tuples):
        insert_query = self.generate_insert_query(object_id, m)
        execute_batch(cursor, insert_query, data_tuples)
        self.log(f"Data appended to table '{object_id}'.")
    
    def replace_data(self, cursor, object_id, m, data_tuples):
        # Delete existing data
        delete_query = sql.SQL("DELETE FROM {table}").format(table=sql.Identifier(object_id))
        cursor.execute(delete_query)
        self.log(f"Existing data deleted from table '{object_id}'.")
    
        # Insert new data
        self.append_data(cursor, object_id, m, data_tuples)
    
    # endregion
    # end region load_data
    # region other_functions
    # end region other_functions
#end region class
