from src.general_scraper import GeneralScraper
from datetime import datetime
from random import uniform 
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import StaleElementReferenceException, TimeoutException
from time import sleep 

class selfLibraryScraper(GeneralScraper):

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
    
    def process_cv_library_job_links(self):
        job_url_list = []
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
                        job_url_list.append(url)
                        self.scroll_to_window(job_card)
                        sleep(uniform(1, 5))
                    except StaleElementReferenceException:
                        # If the job_card becomes stale, refetch the URL element
                        url_element = WebDriverWait(self.driver, 5).until(
                            EC.presence_of_element_located((By.XPATH, self.scraper_config['jobs']['start_extraction']['extract_data']['job_url']))
                        )
                        url = url_element.get_attribute('href')
                        job_url_list.append(url)
                        self.scroll_to_window(job_card)
                        sleep(uniform(1, 5))
                        
                # Locate and click the 'next' button for pagination
                sleep(2)
                next_page = self.click_button_on_page(self.scraper_config['base_config']['next_page_xpath'])
                
                if not next_page:
                    break
                count += 1
                
            except StaleElementReferenceException:
                self.driver.refresh()  
                sleep(3)

        print(job_url_list)

