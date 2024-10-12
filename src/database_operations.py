from sqlalchemy import create_engine
from sqlalchemy import inspect
from sqlalchemy.engine import Engine
from sqlalchemy import text
from sqlalchemy.orm import sessionmaker
from sqlalchemy import MetaData, Table, Column, VARCHAR, DATE, FLOAT, SMALLINT, BOOLEAN, TIME, NUMERIC, TIMESTAMP, INTEGER, UUID, DATETIME, DECIMAL
from pandas import DataFrame
import pandas as pd

import yaml 

class DatabaseOperations: 

    def __init__(self):
        self.type_mapping =  {
            "VARCHAR": VARCHAR,
            "DATE": DATE,
            "FLOAT": FLOAT,
            "SMALLINT": SMALLINT,
            "BOOLEAN": BOOLEAN,
            "TIME": TIME,
            "NUMERIC": NUMERIC,
            "TIMESTAMP": TIMESTAMP,
            "INTEGER": INTEGER,
            "UUID": UUID, 
            "DECIMAL": DECIMAL,
            "DATETIME": DATETIME 
        }

    def load_db_credentials(self, config_path : str):
        """
        Method to load databae credentials from a .yaml file 

        Parameters: 

        config_path : str 

        The file path to the configuration file

        Returns: 

        config : dict 

        A dictionary of database credentials 
        """
        try:
            with open(config_path, 'r') as file:
                config = yaml.safe_load(file)
                print(type(config))
            return config
        except FileNotFoundError:
            print(f"Configuration file {config_path} not found.")
         

    def connect(self, config_file : dict, connect_to_database=False, new_db_name=None):
        DATABASE_TYPE = config_file['DATABASE_TYPE']
        DBAPI = config_file['DBAPI']
        USER = config_file['USER']
        PASSWORD = config_file['PASSWORD']
        ENDPOINT = config_file['ENDPOINT']
        PORT = config_file['PORT']
        DATABASE = config_file['DATABASE']

        # If connecting to a database on the server
        if connect_to_database:
            if not new_db_name:
                raise ValueError ("New database name not provided")
            db_engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{ENDPOINT}:{PORT}/{new_db_name}?client_encoding=utf8", echo=True)
            return db_engine

        # Otherwise connect to the server directly. 
        else:
            db_engine_server = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{ENDPOINT}:{PORT}", echo=True)
            return db_engine_server
    
    def list_db_tables(self, engine: Engine):
        '''
        Method to list the current database tables inside the database 

        Parameters: 

        engine : Engine 
        A sqlalchemy Engine object. 

        Returns: 

        database_tables : list 

        A list of strings which represent the list of tables within the database. 
        '''
        inspector = inspect(engine)
        database_tables = inspector.get_table_names()

        return database_tables

    def create_database(self, engine : Engine, database_name : str):
        """
        Method to create a database. 

        Parameters: 

        engine : Engine 
        
        A sqlalchemy Engine object 

        database_name : str 

        The name of the database to be created

        If the database is not present on the server, it will be created. 
        If the database is present, then it will be skipped. 
        """
        with engine.connect() as database_engine:
            if database_engine:
                # If the database engine is present, then set the isolation level to autocommit 
                database_engine.execution_options(isolation_level="AUTOCOMMIT")
                # Call the database_name_check method to check if the database is present
                result = self.database_name_check(engine, database_name)

                database_exists = result.scalar() # Scalar result returns either 0 or 1
                print(database_exists)
                # If this statement is True, then the database already exists
                if database_exists:
                    print(f'{database_name} already exists, skipping creation')

                else:
                    print('Creating database')
                    create_statement = text(f"CREATE DATABASE {database_name}")
                    database_engine.execute(create_statement)
                    print(f"Database {database_name} created successfully")
            else:
                print('Database creation failed')
                raise Exception 

    @staticmethod 
    def database_name_check(target_engine : Engine, database_name : str):
        """
        Method to check if a database is present on a postgres server. 

        Parameters: 

        target_engine : Engine 

        A sqlalchemy Engine object 

        database_name : str 

        The name of the database 

        """
        #TODO: Adjust the method for other database engines
        # Send a SQL statement to return a scalar result. 
        database_check_statement = text(
                    f"""
                        SELECT EXISTS (
                            SELECT 1
                            FROM pg_database
                            WHERE datname = '{database_name}'
                        ) AS database_exists;
                    """
        )
        with target_engine.connect() as target_connection:
            result = target_connection.execute(database_check_statement)
            return result 

    def read_rds_table(self, engine : Engine, table_name : str):
        '''
        Method to read in an rds table from a sql database

        Given an Engine object, the method will read a sql table
        into a pandas dataframe 

        Parameters: 

        engine : Engine 

        A SQLAlchemy Engine object 

        table_name : str 

        The name of the table within the database 

        Returns 

        rds_table : pd.DataFrame 

        A dataframe representing a table from the database
        '''
        
        rds_table = pd.read_sql_table(table_name, engine)
        return rds_table 

    
    def upsert_table(self, database_df : DataFrame, current_df : DataFrame, current_df_id_column_name : str, dimension_column_name : str):
        """
        Method to upsert a table by comparing two dataframes. 

        Parameters: 

        database_df : DataFrame 

        The current dataframe extracted from the database 

        current_df : DataFrame 

        The dataframe to be compared with the dataframe extracted from the current database

        current_df_id_column_name : str

        The id column name inside the current dataframe
        
        dimension_column_name : str

        The column name for the dimension table 

        """
        # Add the lengths of both dataframes together
        total_number_of_records = len(database_df) + len(current_df)
        # Concatenate the dataframe, dropping any duplicate records 
        combined_df = pd.concat([database_df, current_df]).drop_duplicates(subset=[dimension_column_name])

        
        if len(combined_df) == total_number_of_records:
            print('Number of records in new table match combined table')
            # Set the id column in the current dataframe to the highest id column inside the database_df 
            highest_id_in_column = database_df[current_df_id_column_name].max() 
            # Sets the current id to the highest id in the new dataframe + 1 to avoid raising a primary key error
            current_df[current_df_id_column_name] = current_df.index + highest_id_in_column + 1
            return current_df 

        else:
            print('Number of records in new table do not match combined table')
            # Set the id column in the current dataframe to the highest id column inside the database_df 
            highest_id_in_column = database_df[current_df_id_column_name].max() 
            # Sets the current id to the highest id in the new dataframe + 1 to avoid raising a primary key error
            current_df[current_df_id_column_name] = current_df.index + highest_id_in_column + 1
            # reconcatentate the dataframe 
            combined_df = pd.concat([database_df, current_df]).drop_duplicates(subset=[dimension_column_name])
            # Filter the combined dataframe to include only new records from the current dataframe
            new_records_df = combined_df[~combined_df[current_df_id_column_name].isin(database_df[current_df_id_column_name])]
            # Resetting the index 
            new_records_df.reset_index(drop=True)
            return new_records_df 


    
    def parse_column_type(self, column_type : str):
        """
        Method to parse a column type from a string 

        Parameters: 

        column_type : str 

        The type of column in a string format 

        Returns 
        column_type_object : dict 

        A dictionary which matches the column type to an integer 

        """
        # Split the desired column name by the first occurence of the '(' character
        # NOTE: the *column_parameters will set everything else that is split to a list. 
        column_type_name, *column_parameters = column_type.split('(')
        # Strip the Whitespace from the column_name 
        column_type_name = column_type_name.strip()
        # For the column_parameters variable select the first element of the list,
        # then remove the last character from the list. 
        # Afterwards, apply the split method to split the rest of the string by ','s
        # If the column_parameters variable is empty return an empty list
        column_parameters = column_parameters[0][:-1].split(',') if column_parameters else []
        # Map the variable to a SQLAlchemy Type
        column_type_object = self.type_mapping[column_type_name]
        # If condition for if column_parameters is not empty
        if column_parameters:
            # convert each value in the column_parameter variable to an integer
            column_type_object = column_type_object(*map(int, column_parameters))
        return column_type_object

    def generate_table_schema(self, table_name : str, columns : dict):
        """
        Method to generate the schema for a desired table name
        Utilising SQLAlchemy's ORM syntax 

        Parameters: 

        table_name : str 

        The name of the table. 

        columns : dict 

        A dictionary containing key-value pairs for each of the columns

        Returns: 

        table : Table 

        A Table object representing the table to be created inside the database
        """
        # Create a MetaData() object
        metadata = MetaData()
        table_columns = []
        # Iterate through the columns dictionary
        for column_name, column_type in columns.items():
            # In each iteration, call the parse_column_type function
            column_type_object = self.parse_column_type(column_type)
            # Append the tuple to the list of table_columns
            table_columns.append(Column(column_name, column_type_object))
        # Create a Table object based on the metadata and schema of the columns dictionary
        table = Table(table_name, metadata, *table_columns)

        # Print detailed schema information
        print(f"Table: {table_name}")
        for column in table.columns:
            print(f"Column: {column.name}, Type: {column.type}")
        
        return table
    
    def send_data_to_database(self, dataframe : DataFrame, engine : Engine, table_name : str, condition: str, schema_config : dict):
        """
        Method to send data to a database given a pandas DataFrame object 

        Parameters: 

        dataframe : DataFrame 

        A pandas DataFrame object 

        engine : Engine 

        A sqlalchemy Engine object 

        table_name : str 

        The name of the table to be uploaded 

        condition : str 

        The table condition for the table to be uploaded. 
        Options are 'append', 'replace' and 'fail' 

        schema_config : dict 

        A dictionary containing the configuration of the schema for the table
        Found within a configuration file
        """
        try:
            column_types = schema_config["schemas"]["tables"][table_name]
            table_schema = self.generate_table_schema(table_name, column_types)
            with engine.begin() as connection:
                dataframe.to_sql(name=table_name, con=connection, if_exists=condition, index=False, dtype={col.name: col.type for col in table_schema.columns})
                print(f'Successfully uploaded {table_name} to the database.')
        except Exception as e:
            print(f"An error occurred: {e}")
            raise e


    def execute_sql(self, sql_file_path : str, engine : Engine):
        '''
        Method to execute a sql statement on a database given a sqlalchemy Engine object

        Parameters: 

        sql_file_path : str 

        The file path to the sql script 

        engine : Engine 

        A sqlalchemy Engine object 

        Returns: 
        None 
        '''
        sql_session = sessionmaker(bind=engine)
        session = sql_session()

        # Open the file pathway to the sql script. 
        with open(sql_file_path, "r") as file: 
            sql_statement = file.read()

        session.execute(text(sql_statement))
        session.commit() 
        session.close() 

    def update_ids(self, engine : Engine, id_column_name : str, column_name : str, table_name : str): 
        """
        Method to update the id column within a database

        Parameters: 

        engine : Engine 

        A sqlalchemy Engine object 

        id_column_name : str 
        
        The name of the id column 

        column_name : str 

        The name of a column inside the database

        table_name : str 

        The name of the table inside the database 

        """
        sql_session = sessionmaker(bind=engine)
        session = sql_session() 

        sql_statement = f"""WITH duplicates AS (
                            SELECT {id_column_name}
                            FROM (
                                SELECT {id_column_name}, {column_name},
                                    ROW_NUMBER() OVER (PARTITION BY {column_name} ORDER BY {id_column_name}) AS row_num
                                FROM {table_name}
                            ) AS subquery
                            WHERE row_num > 1
                            )

                            DELETE FROM {table_name}
                            WHERE {id_column_name} IN (SELECT {id_column_name} FROM duplicates);"""
        session.execute(text(sql_statement))
        session.commit() 
        session.close() 

    def reset_ids(self, engine : Engine, id_column_name : str, table_name : str): 
        """
        Method to update the id column within a database

        Parameters: 

        engine : Engine 

        A sqlalchemy Engine object 

        id_column_name : str 
        
        The name of the id column 

        table_name : str 

        The name of the table inside the database 

        """
        sql_session = sessionmaker(bind=engine)
        session = sql_session() 

        sql_statement = f"""WITH reset_ids AS (
                            SELECT {id_column_name}, ROW_NUMBER() OVER (ORDER BY {id_column_name}) AS new_id
                            FROM {table_name}
                            )

                            UPDATE {table_name}
                            SET {id_column_name} = reset_ids.new_id
                            FROM reset_ids
                            WHERE {table_name}.{id_column_name} = reset_ids.{id_column_name};"""
        session.execute(text(sql_statement))
        session.commit() 
        session.close() 

    