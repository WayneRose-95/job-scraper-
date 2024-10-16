from src.general_scraper import GeneralScraper
from datetime import datetime
from random import uniform 
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import StaleElementReferenceException, TimeoutException
from time import sleep 

class CVLibraryScraper(GeneralScraper):

    def __init__(self, base_url : str, scraper_config_filename : str, driver_config_file : str, file_type : str = 'yaml', website_options=False):
        super().__init__(driver_config_file, file_type, website_options=website_options) 
        self.base_url = base_url 
        self.all_data_list = []
        self.scraper_config = self.load_scraper_config(scraper_config_filename, file_type=file_type)

    def load_scraper_config(self, scraper_config_path : str, file_type : str):

        return self.load_config_file(
            scraper_config_path, 
            file_type 
        )

    def bypass_shadow_root(self, shadow_root_script, element_description : str): 
        
        try:
            container_element = self.driver.find_element(By.CSS_SELECTOR, self.scraper_config['jobs']['dismiss_element']['accept_cookies_container'])

            accept_button = self.driver.execute_script(shadow_root_script, container_element)
            if accept_button: 
                accept_button.click() 
                sleep(uniform(0.5, 1.5))
                print(f"{element_description} dismissed.")
        except TimeoutException:
            print(f"No {element_description} found to dismiss.")
        except Exception as e:
            print(f"Error dismissing {element_description}: {e}")


        pass 

    def extract_data_from_webpage(self, webpage_config_dict : dict):

        sleep(uniform(2, 4))
        data = {}

        for key, value in webpage_config_dict.items():

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

