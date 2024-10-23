from datetime import datetime
from random import uniform 
from src.general_scraper import GeneralScraper
from selenium.webdriver.common.by import By
from selenium.common.exceptions import StaleElementReferenceException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from time import sleep 
import pandas as pd 

class ReedScraper(GeneralScraper):

    def __init__(self, base_url : str, scraper_config_filename : str, driver_config_file : str, file_type : str = 'yaml', website_options=False):
        super().__init__(driver_config_file, file_type, website_options=website_options) 
        self.base_url = base_url 
        self.all_data_list = []
        self.scraper_config = self.load_reed_scraper_config(scraper_config_filename, file_type)

    def load_reed_scraper_config(self, scraper_config_path : str, file_type : str):

        return self.load_config_file(
            scraper_config_path, 
            file_type 
        )
    
    def collect_information_from_page(self, webpage_dict : dict):
        reed_dict = {}
        for key, value in webpage_dict.items():
            try:
                if 'main_container' in key:
                    continue
                if 'job_url' in key:
                    reed_dict[key] = self.driver.current_url
                elif 'date_extracted' in key:
                    reed_dict[key] = datetime.now()
                elif "website_name" in key:
                    reed_dict[key] = value
                else: 
                    web_element = self.driver.find_element(By.XPATH, value)
                    reed_dict[key] = web_element.text 

            except:
                reed_dict[key] = 'N/A'

        return reed_dict  

    def set_pagination(self, webpage_element_dict : dict, number_of_pages : int = None):
        '''
        Method to determine how scraper navigates through the webpage given a dictionary of options 

        Parameters
        ---------- 

            webpage_element_dict : dict 

                A dictionary of key-value pairs containing xpaths from a .json file

            number_of_pages : int = None 

                The number of pages to be scraped. Default (None)

                NOTE: If the number_of_pages is None then the entire section will be scraped. 

        Returns 
        -------

            None 

        '''
    pass 

    def process_reed_job_links(self):

        list_of_urls = []
        number_of_pages = self.scraper_config['base_config']['number_of_pages']
        count = 0 

        while count < number_of_pages:
            try:
                # Refetch job cards for each page iteration
                list_of_job_cards = WebDriverWait(self.driver, 10).until(
                    EC.presence_of_all_elements_located((By.XPATH, self.scraper_config['jobs']['start_extraction']['extract_data']['main_container']))
                )
                for job_card in list_of_job_cards:
                    try:
                        url_element = job_card.find_element(By.XPATH, self.scraper_config['jobs']['start_extraction']['extract_data']['job_url'])
                        url = url_element.get_attribute('href')
                        list_of_urls.append(url)
                        self.scroll_to_window(job_card)
                        sleep(uniform(1, 5))
                    except StaleElementReferenceException:
                        # If the job_card becomes stale, refetch the URL element
                        url_element = WebDriverWait(self.driver, 5).until(
                            EC.presence_of_element_located((By.XPATH, self.scraper_config['jobs']['start_extraction']['extract_data']['job_url']))
                        )
                        url = url_element.get_attribute('href')
                        list_of_urls.append(url)
                        self.scroll_to_window(job_card)
                        sleep(uniform(1, 5))
                        
                # Locate and click the 'next' button for pagination
                # next_page_element = self.driver.find_element(By.XPATH, "///div[@class='card pagination_pagination__DChuV']/header") 
                #                                                     # //*[@id="__next"]/div[3]/div/div[3]/main/div[29]/header
                # next_page_element.location_once_scrolled_into_view
                sleep(2)
                next_page = self.click_button_on_page(self.scraper_config['jobs']['scroll_down']['next_page_xpath'])
                
                if not next_page:
                    break
                count += 1
                
            except StaleElementReferenceException:
                self.driver.refresh()  
                sleep(3)

        print(list_of_urls)
        return list(set(list_of_urls))
    
    def extract_job_data(self, list_of_urls : list): 

        for item in list_of_urls: 
            self.driver.get(item)
            sleep(uniform(2, 4))
            webpage_dict = self.collect_information_from_page(self.scraper_config['jobs']['start_extraction']['extract_data'])
            sleep(uniform(2, 5))
            self.all_data_list.append(webpage_dict)
        
        return self.all_data_list

    def run_process(self, job_title : str):

        self.land_first_page(self.base_url)
        sleep(uniform(2,4))
        cookies_button = self.dismiss_element(self.scraper_config['base_config']['cookies_path'], 'Cookies Content')
        sleep(uniform(2,4))
        self.interact_with_search_bar(self.scraper_config['jobs']['landing_page']['interact_with_searchbar_find_job'], job_title)
        unique_list_of_urls = self.process_reed_job_links()
        self.extract_job_data(unique_list_of_urls)

    def reed_output_to_dataframe(self):
        df = pd.DataFrame(self.all_data_list)
        return df 
        pass 
    