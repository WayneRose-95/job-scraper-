from sqlalchemy import create_engine
from sqlalchemy import inspect
from sqlalchemy.engine import Engine
from sqlalchemy import text
from sqlalchemy.orm import sessionmaker
from sqlalchemy import MetaData, Table, Column, VARCHAR, DATE, FLOAT, SMALLINT, BOOLEAN, TIME, NUMERIC, TIMESTAMP, INTEGER, UUID, DATETIME
from pandas import DataFrame

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
        pass 

    def upsert_table(self):
        pass 


    
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

    def update_ids(self):
        pass 

    def reset_ids(self):
        pass 

    