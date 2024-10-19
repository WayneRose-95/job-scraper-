from collections import Counter
from datetime import datetime 
from pandas import DataFrame
from pandas import Series
from src.data_processing import S3DataProcessing
from src.data_processing import DataFrameManipulation
from src.database_operations import DatabaseOperations
from src.indeed_scraper import IndeedScraper
from src.reed_scraper import ReedScraper
from src.cv_library_scraper import CVLibraryScraper
from src.totaljobs_scraper import TotalJobsScraper
from sqlalchemy.engine import Engine

current_date = datetime.now() 
indeed_instance = IndeedScraper("https://uk.indeed.com/", 'config/indeed_config.json', 'config/options_config.yaml')
reed_instance = ReedScraper("https://www.reed.co.uk/", "config/reed_config.json", 'config/options_config.yaml')
cv_instance = CVLibraryScraper("https://www.cv-library.co.uk/", "config/cv-library-config.json", "config/options_config.yaml")
totaljobs_instance = TotalJobsScraper("https://www.totaljobs.com/", "config/totaljobs_config.json", "config/options_config.yaml")
data_processor = S3DataProcessing('job-scraper-data-bucket')
dataframe_manipulation = DataFrameManipulation() 
operator = DatabaseOperations()
indeed_scraper_config = indeed_instance.scraper_config
reed_scraper_config = reed_instance.scraper_config
cv_library_config = cv_instance.scraper_config 
totaljobs_config = totaljobs_instance.scraper_config
target_db_config = operator.load_db_credentials('config/db_creds.yaml')
database_schema = operator.load_db_credentials('config/database_schema.yaml')
database_name = target_db_config['DATABASE']
print(database_name)

def scrape_indeed(job_titles : list, number_of_pages: int = None):
    """
    Function to extract job information from indeed. 

    Parameters
    ----------
        job_titles (list): 
            A list of job titles. 
            Job titles are read in from the indeed_config file 

        number_of_pages (int, optional): 
            The number of pages to scrape per each job title. 
            If the number of pages is None, then the entire website section will be scraped 
            Defaults to None.

    Returns
    -------
        indeed_df : DataFrame
            A dataframe representing data extracted from the website
    """
    for job_title in job_titles:
        indeed_instance.run(
            job_title, 
            number_of_pages=number_of_pages
            )

    indeed_df = indeed_instance.output_to_dataframe() 

    indeed_df.to_csv(indeed_scraper_config['base_config']['output_file_name'], index=False)
    return indeed_df

def scrape_reed(job_titles : list):
    """
    Function to extract job information from reed. 

    Parameters
    ----------
        job_titles (list): 
            A list of job titles. 
            Job titles are read in from the reed_config file 

        number_of_pages (int, optional): 
            The number of pages to scrape per each job title. 
            If the number of pages is None, then the entire website section will be scraped 
            Defaults to None.

    Returns
    -------
        indeed_df : DataFrame
            A dataframe representing data extracted from the website
    """
    for job_title in job_titles:
        reed_instance.run_process(
            job_title 

            )

    reed_df = reed_instance.reed_output_to_dataframe() 

    reed_df.to_csv(reed_scraper_config['base_config']['output_file_name'], index=False)
    return reed_df  

def scrape_totaljobs(job_titles : list):
    """
    Function to extract job information from totaljobs. 

    Parameters
    ----------
        job_titles (list): 
            A list of job titles. 
            Job titles are read in from the totaljobs_config file 

        number_of_pages (int, optional): 
            The number of pages to scrape per each job title. 
            If the number of pages is None, then the entire website section will be scraped 
            Defaults to None.

    Returns
    -------
        indeed_df : DataFrame
            A dataframe representing data extracted from the website
    """
    for job_title in job_titles:
        totaljobs_instance.run_totaljobs_process(
            job_title 
         
            )

    totaljobs_df = totaljobs_instance.totaljobs_output_to_dataframe() 

    totaljobs_df.to_csv(totaljobs_config['base_config']['output_file_name'], index=False)
    return totaljobs_df 

def scrape_cv_library(job_titles : list):
    """
    Function to extract job information from cv_library. 

    Parameters
    ----------
        job_titles (list): 
            A list of job titles. 
            Job titles are read in from the cv_library_config file 


    Returns
    -------
        indeed_df : DataFrame
            A dataframe representing data extracted from the website
    """
    for job_title in job_titles:
        cv_instance.run_main_process(
            job_title
            )

    cv_library_df = cv_instance.cv_library_output_to_dataframe() 

    cv_library_df.to_csv(totaljobs_config['base_config']['output_file_name'], index=False)
    return cv_library_df 

def upload_to_s3(s3_file_name : str):
    """
    Function to upload data to AWS S3 given a file name 

    End-users must have an AWS IAM User with S3 permissions

    Parameters
    ----------
        s3_file_name : str
            The name of the file to send to AWS S3

    Returns 
    -------
        None 
    """
    job_website_name = dataframe_manipulation.extract_from_url(indeed_instance.base_url)

    # Create the file directory 
    file_directory = f'{job_website_name}/{current_date.year}/{current_date.month}/{current_date.day}/'

    s3_object_name = f"{file_directory}{s3_file_name}"

    data_processor.upload_file_to_s3(s3_file_name, s3_object_name, file_directory)

def create_job_database():
    """
    Function to create a database to store information about jobs

    Returns:
        target_database_engine : Engine
            A sqlalchemy Engine object representing the target database
    """
    target_engine = operator.connect(target_db_config, new_db_name=database_name)
    # Create the database
    database = operator.create_database(target_engine, database_name)
    # Once created create another engine to connect to the database itself. 
    target_database_engine = operator.connect(target_db_config, connect_to_database=True, new_db_name=database_name)
    return target_database_engine 
    
def process_dataframes(list_of_s3_filepaths : list):
    """
    Function to process dataframes from an S3 bucket

    Parameters
    ----------
        list_s3_file_paths : str 
            A list representing a file paths inside the S3 bucket 

    Returns:
        dataframe_dict: 
            A dictionary containing dataframes where the keys represent table names and the
            values are the corresponding dataframes.
    """
    # s3_objects = data_processor.list_objects(s3_file_path)

    list_of_responses = []
    for filepath in list_of_s3_filepaths:
        s3_file_path = data_processor.list_objects(filepath)
        list_of_responses.append(s3_file_path)
    print(list_of_responses)

    list_of_objects = []
    for response in list_of_responses:
        object_response = data_processor.read_objects_from_s3(response, 'csv')
        list_of_objects.append(object_response)

    print(list_of_objects)

    # csv_files = data_processor.read_objects_from_s3(s3_objects, 'csv')

    # Reading in a .csv from the s3 bucket
    df = dataframe_manipulation.raw_to_dataframe(list_of_objects)
    # Creating the company_dimension table 
    company_df = dataframe_manipulation.build_dimension_table(df, 'company_name', ["company_name_id", "company_name"])

    # Creating the job_title dimension table 
    job_title_df = dataframe_manipulation.build_dimension_table(df, 'job_title', ['job_title_id', 'job_title'])

    # Creating the description dimension table
    description_df = dataframe_manipulation.build_dimension_table(df, 'job_description', ['job_description_id', 'job_description'])

    # Creating the job_url dimension table
    job_url_df = dataframe_manipulation.build_dimension_table(df, 'job_url', ['job_url_id', 'job_url'])

    # Creating the location dimension table 
    location_df = dataframe_manipulation.build_dimension_table(df, 'location', ['location_id', 'location'])

    # Adds the latitude and longitude columns to the location dataframe
    location_df[['latitude', 'longitude']] = location_df['location'].apply(
        lambda loc: Series(dataframe_manipulation.get_geo_co_ordinates(loc))
    )
    # Creating the time dimension table
    time_dimension_df = dataframe_manipulation.build_dimension_table(df, 'date_extracted', ['date_extracted_id', 'date_extracted'])

    # Creating full time dimension table 

    full_time_dimension_df = dataframe_manipulation.build_time_dimension_table(
        time_dimension_df, 
        'date_extracted', 
        [
            'date_extracted_id', 'date_uuid', 'year', 'month', 'day',
            'date', 'timestamp', 'date_extracted','quarter', 'day_of_week',
            'month_name', 'is_month_start', 'is_month_end', 'is_leap_year', 
            'is_quarter_start', 'is_quarter_end'
        ]
        )
    # Creating website_table 
    website_name_df = dataframe_manipulation.build_dimension_table(df, 'website_name', ['website_name_id', 'website_name'])
    # Adding website url to the table
    website_name_df['website_url'] = [
                                        indeed_scraper_config['base_config']['url'],
                                        reed_scraper_config['base_config']['url'],
                                        totaljobs_config['base_config']['url'],
                                        cv_library_config['base_config']['url']
                                    ]

    

    # Building the fact table 
    fact_table = dataframe_manipulation.build_fact_table(
        df, 
        job_title_df, 
        company_df, 
        location_df, 
        job_url_df, 
        description_df, 
        full_time_dimension_df, 
        website_name_df
        )

    dataframe_dict = {
        "land_job_data" : df,
        "dim_company" : company_df,
        "dim_job_title": job_title_df, 
        "dim_description": description_df, 
        "dim_location": location_df,
        "dim_date": full_time_dimension_df,
        "dim_job_url": job_url_df,
        "dim_website": website_name_df,
        "fact_job_data": fact_table
    }
    return dataframe_dict

def database_table_name_check(dataframe_dict : dict, target_db_engine : Engine):
    """
    Function to check if tables are present inside a database

    Parameters
    ----------
        dataframe_dict (dict): 
            A dictionary containing dataframes where the keys represent table names and the
            values are the corresponding dataframes.

        target_db_engine (Engine):
            A sqlalchemy Engine object for the target database. 

    Returns:
        bool: 
            True if the the number of tables inside the database are the same as inside the dataframe_dict

            False otherwise 
    """

    # Check if the table names are present already 
    current_database_table_names = operator.list_db_tables(target_db_engine)
    database_table_names_to_be_uploaded = list(dataframe_dict.keys())
    # Comparing both lists. If they are the same, then call the process to append data to the dataframes
    if Counter(current_database_table_names) == Counter(database_table_names_to_be_uploaded):
        print("Tables are already present inside database. Upserting data.")
        return True 
    else: 
        return False
    
def filter_dataframes(dataframe_dict : dict, target_engine : Engine, land_job_data_df : DataFrame):
    """
    Function to filter dataframes. 

    Parameters
    ----------
        dataframe_dict (dict): 
            A dictionary containing dataframes where the keys represent table names and the
            values are the corresponding dataframes.

        target_engine (Engine): 
            A sqlalchemy Engine object for the target database.

        land_job_data_df (DataFrame): 
            A dataframe representing the land_job_data dataframe

    Returns
    -------
        dataframe_dict : dict 
            a dictionary containing dataframes where the keys represent table names and the
            values are the corresponding dataframes.
    """
    # Where dataframe_dict represents a dictionary of dataframes to upload

    current_location_df = operator.read_rds_table(target_engine, "dim_location")
    new_location_df = operator.upsert_table(current_location_df, dataframe_dict['dim_location'], 'location_id', 'location')


    current_job_url_df = operator.read_rds_table(target_engine, "dim_job_url")
    new_job_url_df = operator.upsert_table(current_job_url_df, dataframe_dict['dim_job_url'], 'job_url_id', 'job_url')
    
    current_time_dimension_df = operator.read_rds_table(target_engine, "dim_date")
    new_time_dimension_df = operator.upsert_table(current_time_dimension_df, dataframe_dict['dim_date'], 'date_extracted_id', 'date_extracted')
    
    
    current_description_df = operator.read_rds_table(target_engine, 'dim_description')
    new_description_df = operator.upsert_table(current_description_df, dataframe_dict['dim_description'], 'job_description_id', 'job_description')
    
    
    current_company_df = operator.read_rds_table(target_engine, 'dim_company')
    new_company_df = operator.upsert_table(current_company_df, dataframe_dict['dim_company'], 'company_name_id', 'company_name')
  
    
    current_job_title_df = operator.read_rds_table(target_engine, 'dim_job_title')
    new_job_title_df = operator.upsert_table(current_job_title_df, dataframe_dict['dim_job_title'], 'job_title_id', 'job_title')
    
    
    # rebuilding the fact_table 
    #TODO: Rebuilding the fact table does not need to be done here, since the code filters 
    # intends to append to the dimension tables. 
    # new_fact_job_data = dataframe_manipulation.build_fact_table(
    #     land_job_data_df, 
    #     new_job_title_df,
    #     new_company_df,
    #     new_location_df,
    #     new_job_url_df,
    #     new_description_df,
    #     new_time_dimension_df
    # )

    # dataframe_dict['fact_job_data'] = new_fact_job_data

    return dataframe_dict

def update_and_filter_dimension_tables(target_engine : Engine): 
    '''
    Function to update and filter the dimension tables 

    Parameters
    ---------- 
        target_engine : Engine 
            A sqlalchemy engine object pointing to the target database 
    
    Returns
    -------
        None 

    '''

    # for dim_job_title table 
    operator.update_ids(target_engine, "job_title_id", "job_title", "dim_job_title")
    operator.reset_ids(target_engine, "job_title_id", "dim_job_title")

    # for dim_location table 
    operator.update_ids(target_engine, "location_id", "location", "dim_location")
    operator.reset_ids(target_engine, "location_id","dim_location")

    # for dim_job_url table 
    operator.update_ids(target_engine, "job_url_id", "job_url", "dim_job_url")
    operator.reset_ids(target_engine, "job_url_id","dim_job_url")
    
    # for dim_description table 
    operator.update_ids(target_engine, "job_description_id", "job_description", "dim_description")
    operator.reset_ids(target_engine, "job_description_id","dim_description")

    # for dim_company table 
    operator.update_ids(target_engine, "company_name_id", "company_name", "dim_company")
    operator.reset_ids(target_engine, "company_name_id", "dim_company")

    # for dim_website table 
    operator.update_ids(target_engine, "website_name_id", "website_name", "dim_website")
    operator.reset_ids(target_engine, "website_name_id", "dim_website")

def retrieve_dimension_tables(dataframe_dict : dict, target_engine : Engine):
    '''
    The function retrieves dimension tables from a dictionary of dataframes using a specified target
    engine.
    
    Parameters
    ----------
    dataframe_dict : dict
        a dictionary containing dataframes where the keys represent table names and the
        values are the corresponding dataframes.

    target_engine : Engine
         A sqlalchemy engine object pointing to the target database 

    Returns
    -------
        A dictionary containing dimension tables retrieved from the target engine.
    
    '''
    for key, value in dataframe_dict.items(): 
        if "fact" in key:
            continue
        else:
            rds_table = operator.read_rds_table(target_engine, key)
            dataframe_dict[key] = rds_table
    
    return dataframe_dict

def upload_dataframes(dataframe_dict : dict, target_engine : Engine, upload_condition : str, first_load=False):
    '''
    The function `upload_dataframes` uploads dataframes to a database engine based on specified
    conditions, with an option to skip uploading certain dataframes on subsequent loads.
    
    Parameters
    ----------
    dataframe_dict : dict
        A dictionary containing DataFrames where the keys represent the
        names of the DataFrames and the values are the DataFrames themselves.

    target_engine : Engine
        A sqlalchemy engine object pointing to the target database 

    upload_condition : str
        A string to specify how the data should be uploaded to the target database. 
        Viable options are "append", "replace" and "fail"

    first_load, optional
        A boolean flag that indicates whether it is the first time data is being uploaded to the target database. 
        If `first_load` is set to `True`, the function will upload all dataframes in the `dataframe_dict`

    Returns
    ------- 
        None 
    
    '''

    if first_load:
        for key, value in dataframe_dict.items():
            print(key)
            if "land" in key:
                operator.send_data_to_database(value, target_engine, key, "replace", database_schema)
            else:
                operator.send_data_to_database(value, target_engine, key, upload_condition, database_schema)

    else:
        for key, value in dataframe_dict.items():
            print(key)
            if "land" in key:
                operator.send_data_to_database(value, target_engine, key, "replace", database_schema)
            # skip uploading the fact job data dataframe on second load create a seperate function to load the fact data. 
            elif "fact" in key:
                continue
            # skip uploading the dim_website table again to avoid an error. 
            elif "website" in key:
                continue
            else:
                operator.send_data_to_database(value, target_engine, key, upload_condition, database_schema)



if __name__ == "__main__":
    # scrape_indeed(
    #     indeed_scraper_config['base_config']['job_titles']
    #     ,indeed_scraper_config['base_config']['number_of_pages']
    #     ) 
    scrape_reed(reed_scraper_config['base_config']['job_titles']
                , reed_scraper_config['base_config']['number_of_pages'])

    scrape_totaljobs(totaljobs_config['base_config']['job_url']
                    , totaljobs_config['base_config']['number_of_pages'])
    
    scrape_cv_library(cv_library_config['base_config']['job_titles'])
    # upload_to_s3(indeed_scraper_config['base_config']['output_file_name'])
    #NOTE: Using a new database for 1st and 2nd loads jobhubdb_new 
    target_db_engine = create_job_database() 
    dataframe_dictionary = process_dataframes(
                                            [indeed_scraper_config['base_config']['s3_file_path'],
                                             reed_scraper_config['base_config']['s3_file_path'],
                                             totaljobs_config['base_config']['s3_file_path'],
                                            cv_library_config['base_config']['s3_file_path']
                                            ]
                                            )
    land_job_data_table = dataframe_dictionary['land_job_data']

    if database_table_name_check(dataframe_dictionary, target_db_engine) == True:
        # Filter the current dimension tables. 
        filtered_dataframe_dictionary = filter_dataframes(dataframe_dictionary, target_db_engine, land_job_data_table)
        # Upload the filtered dimension tables 
        dimension_table_uploads = upload_dataframes(filtered_dataframe_dictionary, target_db_engine, 'append')
        # Afterwards, update the dimension tables, deleting duplicate records and resetting the id column of each one
        update_and_filter_dimension_tables(target_db_engine) 
        # Retrieve the current dimension tables, adding them to the dataframe_dictionary 
        new_dataframe_dict = retrieve_dimension_tables(dataframe_dictionary, target_db_engine)
        # Rebuild the fact table with the new dataframes 
        new_fact_table = dataframe_manipulation.build_fact_table(
            land_job_data_table, 
            new_dataframe_dict['dim_job_title'],
            new_dataframe_dict['dim_company'],
            new_dataframe_dict['dim_location'],
            new_dataframe_dict['dim_job_url'],
            new_dataframe_dict['dim_description'],
            new_dataframe_dict['dim_date'], 
            new_dataframe_dict['dim_website']
        )
        fact_table_df = new_fact_table
        operator.send_data_to_database(fact_table_df, target_db_engine, "fact_job_data", 'append', database_schema)
    else:
        upload_dataframes(dataframe_dictionary, target_db_engine, 'replace', first_load=True)
        operator.execute_sql('apply_primary_foreign_keys.sql', target_db_engine)
        operator.execute_sql('create_views.sql', target_db_engine)