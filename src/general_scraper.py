import yaml 
import json 

class GeneralScraper:
    '''
    A class containing generic methods for webscraping 

    '''
    def __init__(self, driver_config_file : str, file_type : str = 'yaml'):
        self.driver_config = self.load_driver_config(driver_config_file, file_type)
        pass 

    def load_driver_config(self, config_path : str, file_type : str = 'json' or 'yaml' or 'yml'):
        '''
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
        '''
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
        pass 

    def select_options(self): 
        pass 

    def setup_driver(self):
        pass 
    
    def setup_stealth_driver(self):
        pass 

    def setup_undetected_stealth_driver(self):
        pass 
    
    def land_first_page(self):
        pass 

    def interact_with_search_bar(self):
        pass 

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
