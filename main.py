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

def scrape_indeed():
    indeed_instance.run(
        scraper_config['base_config']['job_titles']
        , scraper_config['base_config']['number_of_pages'] 
        )

    indeed_df = indeed_instance.output_to_dataframe() 

    indeed_df.to_csv('Indeed_Jobs.csv', index=False)
    return indeed_df 

def upload_to_s3(s3_file_name : str):
    job_website_name = dataframe_manipulation.extract_from_url(indeed_instance.base_url)

    # Create the file directory 
    file_directory = f'{job_website_name}/{current_date.year}/{current_date.month}/{current_date.day}/'

    s3_object_name = f"{file_directory}{s3_file_name}"

    data_processor.upload_file_to_s3(s3_file_name, s3_object_name, file_directory)


if __name__ == "__main__":
    scrape_indeed() 
    upload_to_s3('indeed_jobs.csv')