# region imports
from dc_sdk import errors
from psycopg2 import sql
from psycopg2.extras import execute_values
import json
import psycopg2
import requests
import urllib.parse
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
        token_url = "https://api.hubapi.com/oauth/v1/token"
        headers = {
            "Content-Type": "application/x-www-form-urlencoded;charset=utf-8"
        }
        
        try:
            if 'refresh_token' in self.credentials:
                print('Using refresh token flow')
                data = {
                    "grant_type": "refresh_token",
                    "client_id": self.credentials['client_id'],
                    "client_secret": self.credentials['client_secret'],
                    "refresh_token": self.credentials['refresh_token']
                }
            elif 'authorization_code' in self.credentials:
                print('Using authorization code flow')
                data = {
                    "grant_type": "authorization_code",
                    "client_id": self.credentials['client_id'],
                    "client_secret": self.credentials['client_secret'],
                    "redirect_uri": self.credentials['redirect_uri'],
                    "code": self.credentials['authorization_code']
                }
            else:
                return False, 'No valid authentication flow found'
    
            response = requests.post(token_url, headers=headers, data=data)
            response.raise_for_status()
            token_info = response.json()
    
            self.credentials['access_token'] = token_info['access_token']
            if 'refresh_token' in token_info:
                self.credentials['refresh_token'] = token_info['refresh_token']
    
            print('Authentication successful')
            return True, token_info
    
        except requests.exceptions.HTTPError as errh:
            print ("Http Error:",errh)
            return False, str(errh)
        except requests.exceptions.ConnectionError as errc:
            print ("Error Connecting:",errc)
            return False, str(errc)
        except requests.exceptions.Timeout as errt:
            print ("Timeout Error:",errt)
            return False, str(errt)
        except requests.exceptions.RequestException as err:
            print ("Something went wrong",err)
            return False, str(err)
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
        access_token = self.credentials['access_token']
        headers = {
            'Authorization': 'Bearer ' + access_token,
            'Content-Type': 'application/json'
        }
        url = 'https://api.hubapi.com/crm/v3/objects'
    
        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
    
            data = response.json()
    
            objects = []
            for item in data['results']:
                obj = {
                    'object_id': item['id'],
                    'object_name': item['properties']['name'],
                    'object_label': item['properties']['label'],
                    'object_group': item['properties']['group']
                }
                objects.append(obj)
    
            return objects
    
        except requests.exceptions.HTTPError as errh:
            print ("HTTP Error:",errh)
        except requests.exceptions.ConnectionError as errc:
            print ("Error Connecting:",errc)
        except requests.exceptions.Timeout as errt:
            print ("Timeout Error:",errt)
        except requests.exceptions.RequestException as err:
            print ("Something went wrong",err)
    # end region get_objects
    # region get_fields
    def get_fields(self, object_id):
        api_base_url = 'https://api.hubapi.com/crm/v3/schemas'
        object_id = urllib.parse.quote_plus(object_id)
        url = f'{api_base_url}/{object_id}/fields'
        headers = {'Authorization': f'Bearer {self.credentials["access_token"]}'}
    
        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
        except requests.exceptions.HTTPError as err:
            print(f'HTTP error occurred: {err}')
            return None
        except Exception as err:
            print(f'Other error occurred: {err}')
            return None
    
        fields = []
        for field in response.json()['results']:
            fields.append({
                'field_id': field['id'],
                'field_name': field['name'],
                'field_label': field['label'],
                'data_type': field['dataType'],
                'size': field.get('size')  # Some fields might not have a size.
            })
    
        return fields
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
    def load_data(self, data, object_id, m, update_method, batch_number, total_batches):
        # Establish connection
        conn = psycopg2.connect(self.api_base_url)
        cur = conn.cursor()
        
        try:
            # Check if table exists for the first batch
            if batch_number == 0:
                cur.execute(f"SELECT to_regclass('{object_id}')")
                if cur.fetchone()[0] is None:
                    print(f"Table {object_id} does not exist. Creating it now.")
                    # Create table based on mapping
                    create_query = "CREATE TABLE {} (".format(object_id)
                    for column in m:
                        create_query += "{} {}({}), ".format(column["destination_field_id"], column["datatype"], column["size"])
                    create_query = create_query.rstrip(", ") + ")"
                    cur.execute(create_query)
                    print(f"Table {object_id} created successfully.")
            
            # Prepare data for insertion
            insert_data = [tuple(row.values()) for row in data]
    
            # Generate insert query
            insert_query = sql.SQL("INSERT INTO {} ({}) VALUES %s").format(
                sql.Identifier(object_id),
                sql.SQL(',').join(map(sql.Identifier, [col["destination_field_id"] for col in m]))
            )
    
            # Handle update methods
            if update_method == 0:  # Append
                execute_values(cur, insert_query, insert_data)
                print(f"Data appended to {object_id} successfully.")
            elif update_method == 1:  # Replace
                cur.execute(f"DELETE FROM {object_id}")
                execute_values(cur, insert_query, insert_data)
                print(f"Data in {object_id} replaced successfully.")
            elif update_method == 2:  # Upsert
                raise Exception("Upsert operation not supported by PostgreSQL.")
            else:
                raise ValueError("Invalid update_method. Expected 0 (Append), 1 (Replace), or 2 (Upsert).")
    
            # Commit transaction
            conn.commit()
    
        except Exception as e:
            # Rollback in case of error
            conn.rollback()
            raise Exception("An error occurred while loading data: " + str(e))
    
        finally:
            # Close cursor and connection
            cur.close()
            conn.close()
    
        print(f"Batch {batch_number+1} of {total_batches} loaded successfully.")
    # end region load_data
    # region other_functions
    # end region other_functions
#end region class
