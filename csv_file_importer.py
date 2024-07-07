import os
import argparse
import csv
import configparser
import mysql.connector
from mysql.connector import Error

import configparser

class DatabaseConfig:
    def __init__(self, filename='db_config.ini', section='mysql'):
        # Initialize the DatabaseConfig class with a filename and section
        # Read the database configuration from the file and section specified
        self.config = self._read_db_config(filename, section)
        # Extract and store the 'database' value from the configuration
        self.database = self.config.pop('database', None)

    def _read_db_config(self, filename, section):
        # Create a parser to read the configuration file
        parser = configparser.ConfigParser()
        # Read the file
        parser.read(filename)

        db_config = {}
        # Check if the section exists in the configuration file
        if parser.has_section(section):
            # Retrieve all items (key-value pairs) from the section
            items = parser.items(section)
            for item in items:
                # Store each item in the db_config dictionary
                db_config[item[0]] = item[1]
        else:
            # Raise an exception if the section is not found
            raise Exception(f'{section} not found in the {filename} file')

        # Return the configuration dictionary
        return db_config

    def get_config(self):
        # Return a copy of the configuration dictionary (excluding 'database')
        return self.config.copy()

    def get_database(self):
        # Return the value of the 'database' key
        return self.database

    def set_database(self, database):
        # Set a new value for the 'database' key
        self.database = database


class CSVToMySQLImporter:
    def __init__(self, config_file, csv_directory):
        self.db_config = DatabaseConfig(config_file)
        self.csv_directory = csv_directory
        self.connection = None

    def create_connection(self):
        try:
            config = self.db_config.get_config()
            if self.db_config.get_database():
                config['database'] = self.db_config.get_database()
            
            self.connection = mysql.connector.connect(**config)
            print(f"Connected to MySQL Server version {self.connection.get_server_info()}")
            
            if self.db_config.get_database():
                print(f"Connected to database: {self.db_config.get_database()}")
            else:
                print("No specific database selected")
            
        except Error as e:
            print(f"Error connecting to MySQL: {e}")

    def close_connection(self):
        if self.connection:
            self.connection.close()
            print("MySQL connection closed")

    def change_database(self, new_database):
        self.db_config.set_database(new_database)
        if self.connection and self.connection.is_connected():
            self.connection.database = new_database
            print(f"Switched to database: {new_database}")
        else:
            print("Not connected. The new database will be used in the next connection.")

    def import_csv_to_mysql(self, file_path, table_name):
        if not self.connection or not self.connection.is_connected():
            self.create_connection()
        
        # ... (rest of the import logic)

    def import_all_csvs(self):
        self.create_connection()
        if not self.connection:
            return

        # ... (rest of the import all logic)

        self.close_connection()

# Example usage
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='Import CSV files to MySQL database.')
    parser.add_argument('--config', default='db_config.ini', help='Path to database configuration file')
    parser.add_argument('--csv_directory', required=True, help='Directory containing CSV files')

    args = parser.parse_args()

    importer = CSVToMySQLImporter(args.config, args.csv_directory)

    # Example of changing database
    importer.change_database('new_database_name')

    importer.import_all_csvs()






















