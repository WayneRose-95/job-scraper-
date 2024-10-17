from datetime import datetime
from src.general_scraper import GeneralScraper
from time import sleep 
from random import uniform 
class TotalJobsScraper(GeneralScraper):

    def __init__(self, base_url : str, scraper_config_filename : str, driver_config_file : str, file_type : str = 'yaml', website_options=False):
        super().__init__(driver_config_file, file_type, website_options=website_options) 
        self.base_url = base_url 
        self.all_data_list = []
        self.scraper_config = self.load_scraper_config(scraper_config_filename, file_type=file_type)
        pass 
    
    def load_scraper_config(self, scraper_config_path : str, file_type : str):

        return self.load_config_file(
            scraper_config_path, 
            file_type 
        )
    
    def extract_job_details(self, totaljobs_webpage_dict : dict): 
        sleep(uniform(2, 4))
        data = {}

        for key, value in totaljobs_webpage_dict.items():

            if key == "main_container":
                continue
            elif 'url' in key:
                if self.driver.current_url:
                    data[key] = self.driver.current_url
                else:
                    data[key] = 'N/A'
            elif key == 'website_name':
                data[key] = value
            elif 'date' in key:
                data[key] = datetime.today()
            else:
                data[key] = self.extract_element(self.driver, value)

        return data