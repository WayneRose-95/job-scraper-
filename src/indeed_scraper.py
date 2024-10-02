from general_scraper import GeneralScraper
from time import sleep 
from random import uniform
from datetime import datetime
from selenium.common.exceptions import StaleElementReferenceException
from selenium.webdriver.common.by import By
from selenium.webdriver.remote.webelement import WebElement
import pandas as pd 

class IndeedScraper(GeneralScraper):

    def __init__(self, scraper_config_filename : str, driver_config_file : str, file_type : str = 'yaml', website_options=False):
        super.__init__(driver_config_file, file_type, website_options=website_options) 
        self.all_data_list = []
        self.scraper_config = self.load_scraper_config(scraper_config_filename)
        pass 
    
    def load_scraper_config(self, scraper_config_path : str, file_type : str):

        return self.load_config_file(
            scraper_config_path, 
            file_type 
        )
    def extract_data_entry(self, driver, job_paths : dict): #, job_detail_url
        """Creates a data dictionary for each job card element."""

        sleep(uniform(2, 4))
        data = {}

        for key, value in job_paths.items():

            if key == "main_container":
                continue
            elif 'url' in key:
                if self.driver.current_url:
                    data[key] = self.driver.current_url
                else:
                    data[key] = 'N/A'
            elif 'date' in key:
                data[key] = datetime.today()
            else:
                data[key] = self.extract_element(driver, value)

        return data
    
    def process_web_cards(self, job_cards : WebElement, configuration_dict : dict, index : int, container_xpath : str, number_of_retries : int = 3):
        '''
        Method to process a web_element on a given webpage. 

        Parameters: 
        job_cards : WebElement 
        A web_element representing a single card on the webpage 

        configuration_dict : dict 
        A dictionary representing the configuration file for the website 

        index : int 
        The index of the list of webelements 

        container_xpath : str 
        An string representing the container on the webpage 

        number_of_retries : int = 3 
        The number of retries to retrieve the element from the webpage : default 3 retries. 
        '''
        while number_of_retries > 0:
            try:
                # Assign the variable web_card to a web_element in the list. 
                web_card = job_cards[index]
                job_detail_url = self.extract_element(web_card, configuration_dict['job_url'], attribute="href")

                if job_detail_url == "N/A":
                    print("No job URL found, skipping this card.")
                    break  # Skip to the next card

                # Call the scroll_to_window method 
                self.scroll_to_window(web_card)
                sleep(uniform(2, 4))
                self.driver.get(job_detail_url)
                sleep(uniform(2, 4))  # Wait for the page to load if needed

                # Extract the detailed data here
                detailed_data = self.extract_data_entry(self.driver, configuration_dict)
                sleep(uniform(1, 5)) 
                #data.update(detailed_data)
                self.all_data_list.append(detailed_data)
                # Navigate back to the job list page before proceeding to the next job card
                self.driver.back()
                break  # Break out of the retry loop since we've succeeded

            except StaleElementReferenceException:
                #print("StaleElementReferenceException issue, retrying...")
                job_cards = self.driver.find_elements(By.XPATH, container_xpath)  # Re-find job_cards to refresh the references
                web_card = job_cards[index]  # Reacquire the specific card element by its index
                number_of_retries -= 1
                sleep(1)  # Wait a little before retrying

            except Exception as e:
                print(f"An error occurred: {e}")
                raise e 
                # break  # Break out of the loop on other exceptions

    def extract_data(self, job_paths : dict) -> list:
        '''
        Extracts data from the website given a dictionary of key value pairs 
        representing the elements on the website 

        Parameters: 

        job_paths : dict 

        A dictionary of key value pairs 
        representing the elements on the website 

        Returns: 

        self.all_data_list : list 

        A list of all of the data extracted from the website

        '''
        container_xpath = job_paths['main_container']
        self.wait_for_loading(container_xpath)
        job_cards = self.driver.find_elements(By.XPATH, container_xpath)

        # For loop to iterate through each of the elements inside the list of webelements
        for index, _ in enumerate(job_cards):
            # call method to process the job_cards 
            self.process_web_cards(job_cards, job_paths, index, container_xpath)
            pass 

        return self.all_data_list
    
    def set_pagination(self, webpage_element_dict : dict, number_of_pages : int = None):
        '''
        Method to determine how scraper navigates through the webpage given a dictionary of options 

        Parameters: 

        webpage_element_dict : dict 

        A dictionary of key-value pairs containing xpaths from a .json file

        number_of_pages : int = None 

        The number of pages to be scraped. Default (None)

        NOTE: If the number_of_pages is None then the entire section will be scraped. 

        '''
        if number_of_pages is not None:
            count = 0
            while count < number_of_pages:
                self.extract_data(webpage_element_dict)
                next_page_button = self.navigate_to_next_page(self.scraper_config['base_config']['next_page_xpath'])
                if not next_page_button:
                    break
                count+=1
        else:
            self.extract_data(webpage_element_dict)
            next_page_button = self.navigate_to_next_page(self.scraper_config['base_config']['next_page_xpath'])
            while next_page_button:
                self.extract_data(webpage_element_dict)
                self.navigate_to_next_page(self.scraper_config['base_config']['next_page_xpath'])
                if not next_page_button:
                    break 
    
    def decide_and_execute(self, actions : dict, job_title : str, number_of_pages: int = None):
        '''
        Method to decide the order of execution for actions within the scraper based on the .json file 

        Parameters: 

        actions : dict 

        A dictionary containing key-value pairs of actions to take 

        job_title : str 

        The name of the job_title being searched 

        number_of_pages : int = None 

        The number of pages to be scraped. Default (None)

        NOTE: If the number_of_pages is None then the entire section will be scraped. 
        '''
   
        for action_key, value in actions.items():
            self.dismiss_element(self.scraper_config['base_config']['cookies_path'], "Cookies consent")
            self.dismiss_element(self.scraper_config['base_config']['popup_path'], "Pop-up")
            # Handle situations where the value inside the dictionary is not a dictionary
            if not isinstance(value, dict):
                self.wait_for_loading(value)
            # Check the search bar first 
            if "interact_with_searchbar" in action_key:
                self.interact_with_search_bar(value,job_title)   
            elif "click_button" in action_key:
                self.click_button_on_page(value)
            elif "extract_data" in action_key:
                self.set_pagination(value, number_of_pages)
            else:
                print("Please provide a relevant action to be done.")
                break
    
    def run(self, job_title : str, number_of_pages : int = None):
        '''
        Main method to execute the scraping process 

        Parameters: 

        job_title : str

        The job title being scraped 

        number_of_pages : int = None 

        The number of pages to be scraped. Default (None)

        NOTE: If the number_of_pages is None then the entire section will be scraped. 

        '''
        self.land_first_page()
        jobs_actions = self.scraper_config['jobs']
        for job, actions in jobs_actions.items():
            print(f"Processing actions for: {job}")
            self.decide_and_execute(actions, job_title, number_of_pages)

    def output_to_dataframe(self):
        """
        Outputs the data extracted from the website, 
        converting said data into pandas DataFrame

        Returns: 

        df : pd.DataFrame 

        A pandas Dataframe object
        """
        df = pd.DataFrame(self.all_data_list)
        print(df)
        return df 
    
    def close_webpage(self):
        '''
        Closes the webpage of the current browsing session
        '''
        self.driver.quit() 
        

