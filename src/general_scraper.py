from fake_useragent import UserAgent
from selenium.common.exceptions import NoSuchElementException, ElementNotInteractableException
from selenium.webdriver import Chrome
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium import webdriver
from random import uniform
from selenium.webdriver import ChromeService
from selenium_stealth import stealth
from time import sleep
import undetected_chromedriver as uc
from webdriver_manager.chrome import ChromeDriverManager
import json 
import yaml 


class GeneralScraper:
    '''
    A class containing generic methods for webscraping 

    '''
    def __init__(self, driver_config_file : str, file_type : str = 'yaml', website_options=False):
        """
        Attributes

        self.driver : takes in a new instance of the selenium webdriver object 

        """
        self.driver_config = self.load_driver_config(driver_config_file, file_type)
        self.website_options = website_options 

        self.driver_type = self.driver_config['driver_type']
        if self.website_options:
            self.options = self.select_options() 
        
        self.driver = self.select_driver() 


    def load_driver_config(self, config_path : str, file_type : str = 'json' or 'yaml' or 'yml'):
        """
        Method to load in a configuration file to 
        select the type of selenium webdriver object to use

        Parameters: 

        config_path : str 

        The file path to the configuration file 

        file_type : str 

        The file type of the configuration path 

        Returns: 

        config : dict 

        A dictionary of key-value pairs for the configuration file. 

        If the file is not available a FileNotFoundError will be raised
        """
        if file_type == 'json':
            try:
                with open(config_path, 'r') as file:
                    config = json.load(file)
                return config
            except FileNotFoundError:
                print(f"Configuration file {config_path} not found.")
                raise FileNotFoundError
            
        elif file_type == 'yaml' or 'yml':
            try:
                with open(config_path, 'r') as file:
                    config = yaml.safe_load(file)
                return config
            except FileNotFoundError:
                print(f"Configuration file {config_path} not found.")
                raise FileNotFoundError
       


    def select_driver(self):
        '''
        Selects a driver based on what is passed into the driver_config.yaml file 

        Returns: 
        self.driver : WebDriver 
        A selenium WebDriver object
        '''
        if self.driver_type == 'stealth_driver':
            self.driver = self.setup_stealth_driver()
            return self.driver 
        
        elif self.driver_type == 'undetected_stealth_driver':
            self.driver = self.setup_undetected_stealth_driver()
            return self.driver
        
        elif self.driver_type == 'setup_driver':
            self.driver = self.setup_driver()
            return self.driver

        else:
            raise ValueError('Invalid driver selection only stealth_driver, undetected_stealth_driver and setup_driver are valid')

    def select_options(self):
        '''Selects options based on the driver selected from the driver_config.yaml file'''

        if self.driver_type == 'undetected_stealth_driver':
            options = uc.ChromeOptions()
    
            for argument in self.driver_config['setup_driver']['arguments']:
                options.add_argument(argument)
            return options

        elif self.driver_type == 'stealth_driver':
            # Set up Selenium Chrome options
            options = webdriver.ChromeOptions()
            # Adding Arguments from the driver_config.yaml 
            for argument in self.driver_config['setup_driver']['arguments']:
                options.add_argument(argument)
            # Adding experimental arguments from the driver_config.yaml
            for option, argument in self.driver_config['setup_driver']['experimental_options'].items():
                options.add_experimental_option(option, argument)

            return options

        elif self.driver_type == 'setup_driver':
            options = Options()
            # Adding Arguments from the driver_config.yaml 
            for argument in self.driver_config['setup_driver']['arguments']:
                options.add_argument(argument)

            # Adding experimental arguments from the driver_config.yaml
            for option, argument in self.driver_config['setup_driver']['experimental_options'].items():
                options.add_experimental_option(option, argument)
                # Break out of the loop to add only the first experimental option. 
                break
            return options 

    def setup_undetected_stealth_driver(self):
        '''
        Sets up an undetected stealth driver using the undetected-chrome package

        Returns: 
        driver : WebDriver 
        A selenium WebDriver object
        '''
        
        if self.website_options:
            # Creating the undetected driver
            driver = uc.Chrome(options=self.options)

            # Select options from the driver_config.yaml file for selenium-stealth
            selenium_stealth_options = self.driver_config['selenium-stealth']
            # Apply selenium-stealth to further hide the selenium footprint
            stealth(driver,
                    languages=selenium_stealth_options['languages'],
                    vendor=selenium_stealth_options['vendor'],
                    platform=selenium_stealth_options['platform'],  
                    webgl_vendor=selenium_stealth_options['webgl_vendor'],
                    renderer=selenium_stealth_options['renderer'],
                    fix_hairline=selenium_stealth_options['fix_hairline'],
                    run_on_insecure_origins=selenium_stealth_options['run_on_insecure_origins']
                    )
        
            return driver
        
        else:
            # Creating the undetected driver without options
            driver = uc.Chrome()

            # Select options from the driver_config.yaml file for selenium-stealth
            selenium_stealth_options = self.driver_config['selenium-stealth']
            # Apply selenium-stealth to further hide the selenium footprint
            stealth(driver,
                    languages=selenium_stealth_options['languages'],
                    vendor=selenium_stealth_options['vendor'],
                    platform=selenium_stealth_options['platform'],  
                    webgl_vendor=selenium_stealth_options['webgl_vendor'],
                    renderer=selenium_stealth_options['renderer'],
                    fix_hairline=selenium_stealth_options['fix_hairline'],
                    run_on_insecure_origins=selenium_stealth_options['run_on_insecure_origins']
                    )
        
            return driver

        
    def setup_driver(self):
        """
        Sets up the Selenium WebDriver with the appropriate Chrome options.

        if the website_options argument is marked as False, then no ChromeOptions 
        will be used. 

        Returns: 
        Chrome() : WebDriver

        A selenium WebDriver object. 
        """
        if self.website_options:
            ua = UserAgent(browsers=[self.driver_config['setup_driver']['browser']]) 
            service = ChromeService(ChromeDriverManager().install())
            return Chrome(service=service, options=self.options)
        else:
            ua = UserAgent(browsers=[self.driver_config['setup_driver']['browser']])
            service = ChromeService(ChromeDriverManager().install())
            # Setup driver without using the extra command line options
            return Chrome(service=service)


    
    def setup_stealth_driver(self):
        '''

        Sets up an undetected stealth driver using the selenium-stealth package

        Returns: 
        driver : WebDriver 
        A selenium WebDriver object

        '''

        if self.website_options:
            driver = Chrome(
                    service=Service(ChromeDriverManager().install()), options=self.options
                )

            # Set up selenium-stealth
            selenium_stealth_options = self.driver_config['selenium-stealth']
            stealth(driver,
                    languages=selenium_stealth_options['languages'],
                    vendor=selenium_stealth_options['vendor'],
                    platform=selenium_stealth_options['platform'],  
                    webgl_vendor=selenium_stealth_options['webgl_vendor'],
                    renderer=selenium_stealth_options['renderer'],
                    fix_hairline=selenium_stealth_options['fix_hairline']
                    )

            return driver
        
        else:
            # Set up driver without using extra command line options
            driver = Chrome(
            service=Service(ChromeDriverManager().install())
                )

            # Set up selenium-stealth
            selenium_stealth_options = self.driver_config['selenium-stealth']
            stealth(driver,
                    languages=selenium_stealth_options['languages'],
                    vendor=selenium_stealth_options['vendor'],
                    platform=selenium_stealth_options['platform'],  
                    webgl_vendor=selenium_stealth_options['webgl_vendor'],
                    renderer=selenium_stealth_options['renderer'],
                    fix_hairline=selenium_stealth_options['fix_hairline']
                    )

            return driver
    
    def land_first_page(self, url : str):
        '''
        Lands the first page on a website given a url. 

        Parameters: 

        url : str 

        The url of the website 

        '''
        try:
            sleep(uniform(1, 3))
            self.driver.get(url)
            print(f"Successfully navigated to URL: {self.base_url}")
            sleep(uniform(2, 4))
        
        except Exception as e:
            print(f"Error navigating to URL {self.base_url}: {e}")
            raise e

    def click_button_on_page(self, button_xpath : str):
        '''
        Method to click a button on a webpage 

        Parameters: 

        button_xpath : str 

        A string representing a button element on a webpage 

        Returns: button_element : WebElement 

        A Selenium Webdriver WebElement representing the button 
        on the webpage. 

        '''
        try:
            button_element = self.driver.find_element(By.XPATH, button_xpath)
            button_element.click() 
            sleep(uniform(0.5, 2))
            return button_element
        # If it does not exist, raise a NoSuchElementException
        except NoSuchElementException:
            print(f'No such element. Please verify your xpath: {button_xpath}')
            raise NoSuchElementException

        # If the element is not interactable i.e. is not a button, raise an ElementNotInteractableException. 
        except ElementNotInteractableException:
            print('Element is not interactable ({button_xpath}), please verify that a button tag is used')
            raise ElementNotInteractableException
         

    def interact_with_search_bar(self, search_bar_xpath : str, search_bar_text : str, search_bar_button_xpath : str = None):
        """
        Method to interact with the search bar on the webpage 
        
        Parameters: 

        search_bar_xpath : str 

        A string representing the search bar web element

        search_bar_text : str 

        The text to input 
        
        search_bar_button_xpath : str = None

        A string representing the search button on the website 

        """
        # Click the search bar on the webpage 
        search_bar_element = self.click_button_on_page(search_bar_xpath)

        # Input the text into the search bar. 
        for character in search_bar_text:
            search_bar_element.send_keys(character)
            # Sleep for a random period of 0.1 to 2.0 seconds.
            sleep(uniform(0.1, 0.5))

        # Handling logic for when a search button is present on the page
        if search_bar_button_xpath is not None:
            self.click_button_on_page(search_bar_xpath)
            return search_bar_element, search_bar_text

        # If there is no search bar button, then click the Enter key on the webpage
        else:
            search_bar_element.send_keys(Keys.ENTER)
            sleep(uniform(1, 3))
            return search_bar_element, search_bar_text

    def wait_for_loading(self):
        pass 

    def dismiss_element(self): 
        pass 

    def navigate_to_next_page(self):
        pass 

    def extract_element(self): 
        pass 

    def extract_data_entry(self): 
        pass 

    def scroll_to_window(self): 
        pass 
