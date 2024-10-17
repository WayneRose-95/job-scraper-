from datetime import datetime
from src.general_scraper import GeneralScraper
from selenium.webdriver.common.by import By
from selenium.common.exceptions import StaleElementReferenceException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
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
    
    def process_totaljobs_page_links(self):
        list_of_urls = []

        number_of_pages = self.scraper_config['base_config']['number_of_pages']

        count = 0 
        while count < number_of_pages:
            try:
                # Refetch job cards for each page iteration
                list_of_job_cards = WebDriverWait(self.driver, 10).until(
                    EC.presence_of_all_elements_located((By.XPATH, self.scraper_config['jobs']['start_extraction']['extract_data']['main_container']))
                )
                for index, job_card in enumerate(list_of_job_cards):
                    try:
                        url_element = job_card.find_element(By.XPATH, self.scraper_config['jobs']['start_extraction']['extract_data']['job_url'])
                        url = url_element.get_attribute('href')
                        list_of_urls.append(url)
                        self.scroll_to_window(job_card)
                        sleep(uniform(1, 5))
                    except StaleElementReferenceException:
                        sleep(2)
                        self.driver.refresh() 
                        url_element = WebDriverWait(self.driver, 5 )\
                                    .until(EC.presence_of_element_located((By.XPATH, self.scraper_config['jobs']['start_extraction']['extract_data']['job_url'])))
                        url = url_element.get_attribute('href')
                        list_of_urls.append(url)
                        self.scroll_to_window(job_card)
                        sleep(uniform(1, 5))

                sleep(2)
                next_page = self.click_button_on_page(self.scraper_config['base_config']['next_page_xpath'])
                if not next_page: 
                    break 
                count += 1
            
            except StaleElementReferenceException:
                self.driver.refresh()  
                sleep(3)

        print(list_of_urls)
        return list_of_urls 
    
    def extract_totaljobs_information(self, list_of_totaljobs_urls : list):
        for url in list_of_totaljobs_urls:
            sleep(uniform(2,4))
            self.driver.get(url)
            webpage_information = self.extract_job_details(self.scraper_config['jobs']['start_extraction']['extract_data'])
            sleep(uniform(2, 5))
            self.all_data_list.append(webpage_information)