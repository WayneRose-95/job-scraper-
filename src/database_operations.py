from sqlalchemy import create_engine
from sqlalchemy import inspect
from sqlalchemy.engine import Engine
from sqlalchemy import text
from sqlalchemy.orm import sessionmaker
import yaml 

class DatabaseOperations: 

    def __init__(self):
        pass 

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

    def read_rds_table(self):
        pass 

    def upsert_table(self):
        pass 



    def parse_column_type(self):
        pass 

    def generate_table_schema(self):
        pass 

    def send_data_to_database(self):
        pass 

    def execute_sql(self, sql_file_path : str, engine : Engine):
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

    