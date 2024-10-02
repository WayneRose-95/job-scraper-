from src.data_processing import DataProcessing
from src.indeed_scraper import IndeedScraper
from src.database_operations import DatabaseOperations

indeed_instance = IndeedScraper("https://uk.indeed.com/", 'indeed_config.json', 'options_config.yaml')

scraper_config = indeed_instance.scraper_config

indeed_instance.run(
    scraper_config['base_config']['job_titles']
    , scraper_config['base_config']['number_of_pages'] 
    )

indeed_df = indeed_instance.output_to_dataframe() 

indeed_df.to_csv('Indeed_Jobs.csv', index=False)