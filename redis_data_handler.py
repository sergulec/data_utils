import json
import pandas as pd
import redis
from datetime import datetime
import pytz
import logging
# Increase max width of each column (default is 50)
pd.set_option('display.max_colwidth', None)  # None = no limit

# Display all columns in the DataFrame
pd.set_option('display.max_columns', None)  # Show all columns

# Increase the width of the console display
pd.set_option('display.width', 1000)  

logging.basicConfig(level=logging.INFO)
# Configure logger
logger = logging.getLogger(__name__)

class RedisDataHandler:
    """
    The RedisDataHandler class manages interactions with a Redis database, including storing,
    retrieving, and publishing various types of data. It is designed to handle data in multiple 
    formats such as DataFrames, JSON-serialized objects, and plain strings. The class provides 
    flexibility with an option to use Redis pub/sub functionality when required.

    Key functionalities:
    - Publish new rows of a DataFrame to Redis lists and optionally publish via Redis pub/sub.
    - Store and publish JSON-serialized data or plain strings to Redis.
    - Retrieve data stored in Redis in different formats, including DataFrames, JSON, and strings.
    - Manage Redis keys, including deleting keys and getting statistics.
    """
    
    def __init__(self, host='localhost', port=6379, db=0, password=None):
        """
        Initialize the RedisDataHandler with the specified Redis connection details.
        """
        if password:
            self.r = redis.Redis(host=host, port=port, db=db, password=password)
        else:
            self.r = redis.Redis(host=host, port=port, db=db)
        self.est = pytz.timezone('US/Eastern')
        self.today = datetime.now(self.est).strftime("%Y%m%d")
        logger.debug(f"Connected to Redis at {host}:{port}, db: {db}")

    def publish_dataframe(self, df, key, last_published_index, publish=True):
        """
        Publish new rows from a DataFrame to Redis. Optionally publish via Redis pub/sub.

        Parameters:
        - df: The DataFrame to publish.
        - key: The Redis key where the DataFrame rows will be stored.
        - last_published_index: The index of the last published row.
        - publish: If True, also publish to Redis pub/sub. Default is True.
        """
        logger.info(f"Publishing DataFrame to Redis key: {key}")
        logger.info(f"DataFrame received: \n{df}")
        new_rows = df.iloc[last_published_index + 1:]
        for _, row in new_rows.iterrows():
            row_json = row.to_json()
            self.r.rpush(key, row_json)
            logger.debug(f"Stored new row for {key} in Redis: {row_json}")
            if publish:
                self.r.publish(key, row_json)
                logger.debug(f"Published to Redis under key {key}: {row_json}")
        return len(df) - 1

    def publish_dataframe_pipe(self, df, key, publish=True):
        """
        Publish all rows from a DataFrame to Redis. Optionally publish via Redis pub/sub.

        Args:
        - df: The DataFrame to publish.
        - key: The Redis key where the DataFrame rows will be stored.
        - publish: If True, also publish to Redis pub/sub. Default is True.
        
        Returns:
        - int: Total number of rows published.
        """
        logger.info(f"Publishing DataFrame to Redis key: {key}")
        logger.info(f"DataFrame received: \n{df}")
        json_rows = [row.to_json() for _, row in df.iterrows()]  # Convert all rows to JSON
        logger.info(f"Publishing {len(json_rows)} rows to Redis key: {key}")

        if json_rows:
            with self.r.pipeline() as pipe:
                pipe.delete(key)  # Clear the key first
                for row_json in json_rows:
                    pipe.rpush(key, row_json)  # Push each row
                    if publish:
                        pipe.publish(key, row_json)  # Publish via pub/sub
                pipe.execute()
        else:
            logger.info(f"No rows to publish to {key}.")
        
        return len(df)

    def publish_to_redis_json(self, data, key, publish=True):
        """
        Store JSON data in Redis and optionally publish via Redis pub/sub.

        Parameters:
        - data: The data to store and publish (will be serialized to JSON).
        - key: The Redis key where the data will be stored.
        - publish: If True, also 
         
          
           
             to Redis pub/sub. Default is True.
        """
        json_data = json.dumps(data)
        self.r.set(key, json_data)
        logger.debug(f"Stored JSON data in Redis under key {key}: {json_data}")
        if publish:
            self.r.publish(key, json_data)
            logger.debug(f"Published to Redis under key {key}: {json_data}")

    def publish_to_redis_str(self, data, key, publish=True):
        """
        Store a string in Redis and optionally publish via Redis pub/sub.

        Parameters:
        - data: The string to store and publish.
        - key: The Redis key where the string will be stored.
        - publish: If True, also publish to Redis pub/sub. Default is True.
        """
        self.r.set(key, str(data))
        logger.debug(f"Stored string data in Redis under key {key}: {data}")
        if publish:
            self.r.publish(key, str(data))
            logger.debug(f"Published to Redis under key {key}: {data}")
        qb = self.r.get(key)
        logger.debug(f"redis_client.get({key}) = {qb.decode('utf-8')}")

    def retrieve_dataframe_from_redis_old(self, key, timestamp_col='timestamp'):
        """
        Retrieve data from a Redis list and convert it into a DataFrame.

        Parameters:
        - key: The Redis key where the DataFrame rows are stored.
        - timestamp_col: The column name to be used as the timestamp for sorting and de-duplication. 
                        Default is 'timestamp'.

        Returns:
        - DataFrame: A DataFrame created from the Redis list.
        """
        logger.info(f"Retrieving DataFrame from Redis key: {key} with timestamp column: {timestamp_col}")
        # Get all elements from the Redis list
        data = self.r.lrange(key, 0, -1)
        
        # Convert the list of JSON strings to a list of dictionaries
        rows = [json.loads(row.decode('utf-8')) for row in data]
        
        # Create a DataFrame from the list of dictionaries
        df = pd.DataFrame(rows)
        if timestamp_col and timestamp_col in df.columns:
            # Convert to datetime with utc=True
            df[timestamp_col] = pd.to_datetime(df[timestamp_col], utc=True)
        else:
            logger.debug(f"Timestamp column '{timestamp_col}' not found in DataFrame.")


        if timestamp_col not in df.columns:
            raise ValueError(f"Column '{timestamp_col}' not found in the data.")
        
        # Convert the specified timestamp column to a datetime object
        df[timestamp_col] = pd.to_datetime(df[timestamp_col])
        
        # Remove duplicates based on the specified timestamp column and keep the last occurrence
        df = df.drop_duplicates(subset=[timestamp_col], keep='last')
        
        # Sort the DataFrame by the specified timestamp column
        df = df.sort_values(by=timestamp_col)
        
        return df

    def retrieve_dataframe_from_redis(self, key, timestamp_col='timestamp'):
        logger.debug(f"Retrieving DataFrame from Redis key: {key} with timestamp column: {timestamp_col}")
        # Get all elements from the Redis list
        data = self.r.lrange(key, 0, -1)
        
        if not data:
            logger.info("No data retrieved from Redis.")
            # Return an empty DataFrame with the expected timestamp column
            return pd.DataFrame(columns=[timestamp_col])
        
        # Convert the list of JSON strings to a list of dictionaries
        rows = [json.loads(row.decode('utf-8')) for row in data]
        
        # Create a DataFrame from the list of dictionaries
        df = pd.DataFrame(rows)
        
        if timestamp_col in df.columns:
            # Convert to datetime with utc=True
            df[timestamp_col] = pd.to_datetime(df[timestamp_col], utc=True)
        else:
            logger.debug(f"Timestamp column '{timestamp_col}' not found in DataFrame.")
            # If the DataFrame doesn't have the timestamp column, raise an error or handle accordingly
            raise ValueError(f"Column '{timestamp_col}' not found in the data.")
        
        # Continue with the rest of your processing
        df = df.drop_duplicates(subset=[timestamp_col], keep='last')
        df = df.sort_values(by=timestamp_col)
        
        return df
    
    def retrieve_dataframe_from_redis_uniq(self, key, uid_column='id', sort_column='updatedAt', only_today=False):
        logger.debug(f"Retrieving DataFrame from Redis key: {key} with uid column: {uid_column}")
        
        # Get all elements from the Redis list
        data = self.r.lrange(key, 0, -1)
        
        if not data:
            logger.info("No data retrieved from Redis.")
            # Return an empty DataFrame with the expected columns
            return pd.DataFrame(columns=[uid_column, sort_column])
        
        # Convert the list of JSON strings to a list of dictionaries
        rows = [json.loads(row.decode('utf-8')) for row in data]
        
        # Create a DataFrame from the list of dictionaries
        df = pd.DataFrame(rows)
        
        if only_today:
            # Drop NaN values in the sort_column
            df = df.dropna(subset=[sort_column])
            
            # Filter the DataFrame based on today's date using the sort_column (string comparison)
            logger.info(f"Filtering DataFrame with {df.size} by today's date based on column: {sort_column}\n {df.head()}")
            today = datetime.now(self.est).strftime('%Y-%m-%d')
            logger.info(f"Today's date: {today} type: {type(today)}")
            logger.info(f"sort_column: {df[sort_column].head()}\n {df.dtypes}")
            # Filter rows where the date in sort_column matches today's date (string comparison)
            df = df[df[sort_column].str.startswith(today)]
            logger.info(f"Filtered DataFrame to {df.size} rows for today's date. types: \n{df.dtypes}")         
            logger.info(f"df after filtering: \n{df}")
            
            if df.empty:
                return pd.DataFrame(columns=[uid_column, sort_column])
        
        # Ensure uniqueness based on the uid_column
        df = df.drop_duplicates(subset=[uid_column], keep='last')
        
        # Sort the DataFrame by the sort_column
        df = df.sort_values(by=sort_column, ascending=False)
    
        return df


    def retrieve_dataframe_from_redis_uniq_orig(self, key, uid_column='id', sort_column='updatedAt'):
        logger.info(f"Retrieving DataFrame from Redis key: {key} with uid column: {uid_column}")
            
        # Get all elements from the Redis list
        data = self.r.lrange(key, 0, -1)
            
        if not data:
            logger.info("No data retrieved from Redis.")
            # Return an empty DataFrame with the expected timestamp column
            return pd.DataFrame(columns=[uid_column])
            
        # Convert the list of JSON strings to a list of dictionaries
        rows = [json.loads(row.decode('utf-8')) for row in data]
            
        # Create a DataFrame from the list of dictionaries
        df = pd.DataFrame(rows)
            
        # Continue with the rest of your processing
        df = df.drop_duplicates(subset=[uid_column], keep='last')
        df = df.sort_values(by=sort_column, ascending=False)
            
        return df



    def retrieve_json_from_redis(self, key):
        """
        Retrieve and deserialize JSON data from Redis.

        Parameters:
        - key: The Redis key where the JSON data is stored.

        Returns:
        - The deserialized JSON data (e.g., dictionary or list).
        """
        json_data = self.r.get(key)
        if json_data is None:
            return None
        return json.loads(json_data.decode('utf-8'))

    def retrieve_str_from_redis(self, key):
        """
        Retrieve string data from Redis.

        Parameters:
        - key: The Redis key where the string is stored.

        Returns:
        - The string stored under the key.
        """
        data = self.r.get(key)
        if data is None:
            return None
        return data.decode('utf-8')

    def delete_keys(self, keys):
        """
        Delete a single key or a set of keys from Redis.

        Parameters:
        - keys: str or list of str. A single key or a list of keys to delete.

        Returns:
        - dict: A dictionary containing the number of deleted keys and a list of non-existent keys.
        """
        if isinstance(keys, str):
            keys = [keys]

        non_existent_keys = []
        deleted_count = 0

        for key in keys:
            if self.r.exists(key):
                self.r.delete(key)
                deleted_count += 1
                logger.debug(f"Deleted key from Redis: {key}")
            else:
                non_existent_keys.append(key)
                logger.debug(f"Key does not exist in Redis: {key}")

        return {
            'Deleted Keys Count': deleted_count,
            'Non-existent Keys': non_existent_keys
        }

    def get_all_keys(self):
        """
        Get a list of all keys in the Redis database.
        """
        return self.r.keys('*')

    def get_keys_dataframe(self):
        """
        Get a DataFrame containing statistics for all keys in the Redis database.
        """
        all_keys = self.get_all_keys()
        logger.debug(f"Retrieved all keys from Redis: {all_keys}")
        data = []
        for key in all_keys:
            key_str = key.decode('utf-8')
            stats = self.get_key_stats(key_str)
            data.append((key_str, stats['Type'], stats['Number of Items'], stats['Size (Bytes)'], stats['Size (MB)']))

        df = pd.DataFrame(data, columns=['Key', 'Type', 'Number of Items', 'Size (Bytes)', 'Size (MB)'])
        df = df.sort_values(by='Size (MB)', ascending=True)
        return df

    def get_key_stats(self, key):
        """
        Get detailed statistics for a specific Redis key.

        Parameters:
        - key: The Redis key for which to get stats.

        Returns:
        - dict: A dictionary containing the type, number of items, size in bytes, and size in megabytes.
        - str: An error message if the key does not exist.
        """
        if not self.r.exists(key):
            return f"Key '{key}' does not exist in Redis."

        key_type = self.r.type(key).decode('utf-8')
        stats = {
            'Key': key,
            'Type': key_type,
            'Number of Items': 0,
            'Size (Bytes)': 0,
            'Size (MB)': 0.0
        }

        if key_type == 'string':
            size_bytes = self.r.strlen(key)
            stats['Number of Items'] = 1
            stats['Size (Bytes)'] = size_bytes
            stats['Size (MB)'] = size_bytes / (1024 * 1024)

        elif key_type == 'list':
            num_items = self.r.llen(key)
            size_bytes = sum(len(item) for item in self.r.lrange(key, 0, -1))
            stats['Number of Items'] = num_items
            stats['Size (Bytes)'] = size_bytes
            stats['Size (MB)'] = size_bytes / (1024 * 1024)

        elif key_type == 'set':
            num_items = self.r.scard(key)
            size_bytes = sum(len(member) for member in self.r.smembers(key))
            stats['Number of Items'] = num_items
            stats['Size (Bytes)'] = size_bytes
            stats['Size (MB)'] = size_bytes / (1024 * 1024)

        elif key_type == 'zset':
            num_items = self.r.zcard(key)
            size_bytes = sum(len(member) for member, _ in self.r.zscan_iter(key))
            stats['Number of Items'] = num_items
            stats['Size (Bytes)'] = size_bytes
            stats['Size (MB)'] = size_bytes / (1024 * 1024)

        elif key_type == 'hash':
            num_items = self.r.hlen(key)
            size_bytes = sum(len(field) + len(value) for field, value in self.r.hgetall(key).items())
            stats['Number of Items'] = num_items
            stats['Size (Bytes)'] = size_bytes
            stats['Size (MB)'] = size_bytes / (1024 * 1024)

        logger.debug(f"Key stats for {key}: {stats}")
        return stats

