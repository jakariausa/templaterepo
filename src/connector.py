# region imports
from dc_sdk import errors
from psycopg2 import sql
import logging
import psycopg2
import psycopg2.extras
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
    # Required imports
    
    # Inside class
    
    def load_data(self, data, object_id, m, update_method, batch_number, total_batches, primary_key=None):
        # Initialize cursor
        cursor = self.connection.cursor()
    
        # Generate column and value strings for SQL query
        columns, values = self._generate_columns_values(m, data)
    
        try:
            # Append new rows to the table
            if update_method == 0:
                logging.info(f'Appending data to {object_id}...')
                self._append_data(cursor, object_id, columns, values, data)
    
            # Replace existing rows in the table
            elif update_method == 1:
                logging.info(f'Replacing data in {object_id}...')
                self._replace_data(cursor, object_id, columns, values, data)
    
            # Upsert rows into the table
            elif update_method == 2:
                if not primary_key:
                    raise ValueError('Primary key is required for upsert operations')
                logging.info(f'Upserting data into {object_id}...')
                self._upsert_data(cursor, object_id, columns, values, data, primary_key)
    
            # Commit the transaction
            self.connection.commit()
    
            logging.info(f'Successfully loaded data into {object_id}')
            return True
    
        except Exception as e:
            # Rollback the transaction and log the error
            self.connection.rollback()
            logging.error(f'Failed to load data into {object_id}. Error: {str(e)}')
            raise LoadDataError(f'Failed to load data into {object_id}. Error: {str(e)}')
    
        finally:
            cursor.close()
    
    
    def _generate_columns_values(self, m, data):
        columns = ', '.join([d['column'] for d in m])
        values = ', '.join(['%s' for _ in m])
        return columns, values
    
    
    def _append_data(self, cursor, object_id, columns, values, data):
        insert_sql = sql.SQL('INSERT INTO {} ({}) VALUES ({})').format(
            sql.Identifier(object_id),
            sql.SQL(columns),
            sql.SQL(values)
        )
        psycopg2.extras.execute_batch(cursor, insert_sql, data)
    
    
    def _replace_data(self, cursor, object_id, columns, values, data):
        cursor.execute(f'TRUNCATE TABLE {object_id}')
        self._append_data(cursor, object_id, columns, values, data)
    
    
    def _upsert_data(self, cursor, object_id, columns, values, data, primary_key):
        insert_sql = sql.SQL('INSERT INTO {} ({}) VALUES ({}) ON CONFLICT ({}) DO UPDATE SET').format(
            sql.Identifier(object_id),
            sql.SQL(columns),
            sql.SQL(values),
            sql.SQL(', '.join(primary_key))
        )
        psycopg2.extras.execute_batch(cursor, insert_sql, data)
    
    
    class LoadDataError(Exception):
        pass
    # end region load_data
    # region other_functions
    # end region other_functions
#end region class
