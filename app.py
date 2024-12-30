'''Migrating large data from one DB to another DB'''


import psycopg2
import pandas as pd
from sqlalchemy import create_engine
import time
import logging
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

class DatabaseMigration:
    def __init__(self):
        # Setup logging
        logging.basicConfig(level=logging.INFO, filename='log.log', filemode='w', format='%(asctime)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger(__name__)

        # Load configuration from environment variables
        self.source_config = {
            'dbname': os.getenv('SOURCE_DB_NAME'),
            'user': os.getenv('SOURCE_DB_USER'),
            'password': os.getenv('SOURCE_DB_PASSWORD'),
            'host': os.getenv('SOURCE_DB_HOST'),
            'port': os.getenv('SOURCE_DB_PORT')
        }

        self.target_config = {
            'dbname': os.getenv('TARGET_DB_NAME'),
            'user': os.getenv('TARGET_DB_USER'),
            'password': os.getenv('TARGET_DB_PASSWORD'),
            'host': os.getenv('TARGET_DB_HOST'),
            'port': os.getenv('TARGET_DB_PORT')
        }

        self.source_table = os.getenv('SOURCE_TABLE_NAME')
        self.target_table = os.getenv('TARGET_TABLE_NAME')
        self.chunk_size = int(os.getenv('CHUNK_SIZE', 5000))  # Default to 5,000 if not set

        try:
            # Source database connection
            self.source_conn = psycopg2.connect(
                dbname=self.source_config['dbname'],
                user=self.source_config['user'],
                password=self.source_config['password'],
                host=self.source_config['host'],
                port=self.source_config['port']
            )
            self.source_cursor = self.source_conn.cursor()

            # Target database connection (using SQLAlchemy for easier management)
            self.target_engine = create_engine(
                f"postgresql+psycopg2://{self.target_config['user']}:{self.target_config['password']}@{self.target_config['host']}:{self.target_config['port']}/{self.target_config['dbname']}"
            )
        except Exception as e:
            self.logger.error(f"Error in database connection: {str(e)}")
            raise

    def transform_data(self, data_frame):
        """
        Function to apply business transformations on data.
        This is where most business logic will go.
        For example, adding calculated columns, data cleaning, etc.
        """
        self.logger.info("Starting data transformation.")

        try:
            # Remove extra spaces from all columns
            data_frame = data_frame.applymap(lambda x: x.strip() if isinstance(x, str) else x)

            # Replace empty or missing values for specific columns with None (which represents NULL in PostgreSQL)
            if 'customer_lname' in data_frame.columns:
                data_frame['customer_lname'] = data_frame['customer_lname'].replace('', None)

            if 'customer_street' in data_frame.columns:
                data_frame['customer_street'] = data_frame['customer_street'].replace('', None)

            self.logger.info(f"Data transformation complete. Processed {len(data_frame)} rows.")
            return data_frame

        except Exception as e:
                self.logger.error(f"Error in data transformation: {str(e)}")
                raise

    def extract_data_in_chunks(self, source_table, chunk_size=5000):
        """
        Extract data from the source database in chunks.
        """
        offset = 0
        while True:
            # Construct the query to extract all data from the source table
            paginated_query = f"SELECT * FROM {source_table} LIMIT {chunk_size} OFFSET {offset}"
            try:
                self.source_cursor.execute(paginated_query)
                chunk = self.source_cursor.fetchall()

                if not chunk:
                    break

                columns = [desc[0] for desc in self.source_cursor.description]
                df_chunk = pd.DataFrame(chunk, columns=columns)

                yield df_chunk  # Return the current chunk
                offset += chunk_size  # Move to the next chunk
            except Exception as e:
                self.logger.error(f"Error while extracting data from {source_table}: {str(e)}")
                raise

    def load_data_to_target(self, df, target_table):
        """
        Load the transformed data into the target database.
        """
        try:
            # Using pandas to load data into PostgreSQL through SQLAlchemy
            df.to_sql(target_table, con=self.target_engine, index=False, if_exists='append')
            self.logger.info(f"Successfully loaded {len(df)} records to {target_table}.")
        except Exception as e:
            self.logger.error(f"Error while loading data to {target_table}: {str(e)}")
            raise

    def run_migration(self, source_table, target_table, chunk_size=5000):
        """
        Main function to run the migration process.
        """
        start_time = time.time()

        self.logger.info(f"Migration started for {source_table} to {target_table}.")
        try:
            for chunk in self.extract_data_in_chunks(source_table, chunk_size):
                self.logger.info(f"Processing chunk with {len(chunk)} rows...")
                transformed_data = self.transform_data(chunk)
                self.load_data_to_target(transformed_data, target_table)

        except Exception as e:
            self.logger.error(f"Migration failed for {source_table} to {target_table}: {str(e)}")
            raise

        end_time = time.time()
        self.logger.info(f"Migration completed for {source_table} to {target_table} in {end_time - start_time:.2f} seconds.")

    def close_connections(self):
        """
        Close database connections.
        """
        try:
            self.source_cursor.close()
            # This closes the cursor object (source_cursor) associated with the source database connection. A cursor is used to execute SQL queries and fetch results, and closing it is an important step in releasing resources.
            self.source_conn.close()
            self.logger.info("Database connections closed.")
        except Exception as e:
            self.logger.error(f"Error closing database connections: {str(e)}")
            raise


# Instantiate the migration object
migration = DatabaseMigration()

source_table = migration.source_table  # Dynamic source table from environment variable 
target_table = migration.target_table  # Dynamic target table from environment variable

# Run migration
migration.run_migration(source_table, target_table, chunk_size=migration.chunk_size)

# Close the connections
migration.close_connections()