from src.general_scraper import GeneralScraper

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
    
    def collect_information_from_page(self):
        pass 

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
    