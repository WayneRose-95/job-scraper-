from datetime import datetime 
from src.data_processing import S3DataProcessing
from src.data_processing import DataFrameManipulation
from src.database_operations import DatabaseOperations
from src.indeed_scraper import IndeedScraper

current_date = datetime.now() 
indeed_instance = IndeedScraper("https://uk.indeed.com/", 'config/indeed_config.json', 'config/options_config.yaml')
data_processor = S3DataProcessing('job-scraper-data-bucket')
dataframe_manipulation = DataFrameManipulation() 
operator = DatabaseOperations()
scraper_config = indeed_instance.scraper_config
target_db_config = operator.load_db_credentials('config/db_creds.yaml')
database_name = target_db_config['DATABASE']
print(database_name)

def scrape_indeed():
    indeed_instance.run(
        scraper_config['base_config']['job_titles']
        , scraper_config['base_config']['number_of_pages'] 
        )

    indeed_df = indeed_instance.output_to_dataframe() 

    indeed_df.to_csv('indeed_jobs.csv', index=False)
    return indeed_df 

def upload_to_s3(s3_file_name : str):
    job_website_name = dataframe_manipulation.extract_from_url(indeed_instance.base_url)

    # Create the file directory 
    file_directory = f'{job_website_name}/{current_date.year}/{current_date.month}/{current_date.day}/'

    s3_object_name = f"{file_directory}{s3_file_name}"

    data_processor.upload_file_to_s3(s3_file_name, s3_object_name, file_directory)

def create_job_database():
    server_engine = operator.connect(target_db_config, new_db_name=database_name)
    # Create the database
    database = operator.create_database(server_engine, database_name)
    # Once created create another engine to connect to the database itself. 
    target_database_engine = operator.connect(target_db_config, connect_to_database=True, new_db_name=database_name)
    return target_database_engine 
    
def process_dataframes(s3_file_path : str):
    s3_objects = data_processor.list_objects(s3_file_path)
    csv_files = data_processor.read_objects_from_s3(s3_objects, 'csv')

    # Reading in a .csv from the s3 bucket
    df = dataframe_manipulation.raw_to_dataframe(data_processor.list_of_objects)
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
    

    # Building the fact table 
    fact_table = dataframe_manipulation.build_fact_table(df, job_title_df, company_df, location_df, job_url_df, description_df, full_time_dimension_df)

    dataframe_dict = {
        "land_job_data" : df,
        "dim_company" : company_df,
        "dim_job_title": job_title_df, 
        "dim_description": description_df, 
        "dim_location": location_df,
        "dim_date": full_time_dimension_df,
        "dim_job_url": job_url_df,
        "fact_job_data": fact_table
    }
    return dataframe_dict
if __name__ == "__main__":
    # scrape_indeed() 
    # upload_to_s3('indeed_jobs.csv')
    create_job_database() 
    dataframe_dictionary = process_dataframes(f'indeed/{current_date.year}/{current_date.month}/{current_date.day}/')
    land_job_data_table = dataframe_dictionary['land_job_data']
    