from botocore.exceptions import ClientError
from datetime import datetime
from geopy.geocoders import Nominatim
from io import StringIO
from uuid import uuid4
import boto3
import pandas as pd
import re


class S3DataProcessing: 

    def __init__(self, bucket_name : str):
        self.bucket_name = bucket_name
        self.s3_client = boto3.client('s3')
        self.list_of_objects = []
        pass 

    def list_objects(self, file_directory : str):
        """
        
        Parameters 
        ----------
        file_directory (str): 
            The file path to the file directory on the AWS S3 bucket

        Returns
        -------
        s3_response  (dict): 
            A dictionary containing the response from the boto3 api request
        """

        s3_response = self.s3_client.list_objects_v2(
            Bucket=self.bucket_name,
            Prefix=file_directory
        )
        print(s3_response)

        return s3_response
    

    def read_objects_from_s3(self, s3_response : dict, file_type : str):
        '''
        Method to read in objects from an S3 bucket based on a specified file type.
        
        Parameters
        ----------
        s3_response : dict
            A dictionary containing information
        about objects stored within the S3 bucket
        file_type : str
            The type of file to look for within the S3 bucket. 
        
        Returns
        -------
        s3_objects : any 
            the last object retrieved from the S3 bucket that matches
            the specified file type.
        
        '''

        for object in s3_response['Contents']:
            key = object['Key']
            if key.endswith(f".{file_type}"):
                print(key)
                s3_objects = self.s3_client.get_object(
                    Bucket=self.bucket_name,
                    Key=key
                )
                print(s3_objects)
                data = s3_objects['Body']
                self.list_of_objects.append(data)
        
        print(self.list_of_objects)
        return s3_objects
      

    def create_s3_directory(self, website_name : str):
        '''
        Creates a directory structure in an S3 bucket based on the
        website name and current date.
        
        Parameters
        ----------
        website_name : str
            A string representing the name of website. 
            This is the first directory inside the S3 bucket. 
            After which further entries will be structured by date. 
            Year/Month/Day. 
        
        Returns
        -------
        directory_name : str 
            The name of the directory if the directory was successfully created 
            or if it already exists. 
            
            If an error occurs during the process, it returns `None`.
        
        '''
        # Get the current datetime 
        current_date = datetime.now() 
        # Create the file_path
        directory_name = f'{website_name}/{current_date.year}/{current_date.month}/{current_date.day}/'
        placeholder_file = directory_name + "placeholder.txt"

        try: 
            response = self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=placeholder_file,
                Body='b '
            )
            # Return the directory_name to be used as a filepath to upload to s3. 
            print('File directory created')
            return directory_name
        except ClientError as e:
            if e.response['Error']['Code'] == 'KeyAlreadyExists':
                print(f"Directory '{directory_name}' already exists in bucket '{self.bucket_name}'.")
                return directory_name
        except Exception as e:
            print(f"An error occurred: {e}")
            return None
       

    def upload_file_to_s3(self, file_name : str, object_name : str, folder : str):
        '''
        Method to upload a file to an S3 bucket with error handling.
        
        Parameters
        ----------
        file_name : str
            A string that specifies the full path to the file
            on your local system.
        object_name : str
            A string representing the key or path under which the file will be stored in the S3 bucket.
        folder : str
            A string which represents the folder within the S3
            bucket where you want to upload the file. 
        
        '''
        try:
            self.s3_client.upload_file(file_name, self.bucket_name, object_name)
            print(f"Uploaded {file_name} to S3 bucket {self.bucket_name} in folder {folder}.")
        except Exception as e:
            print(f"Failed to upload {file_name} to S3: {e}")
       

class DataFrameManipulation: 

    def raw_to_dataframe(self, list_of_objects : list):
        '''
        Method to read raw data from a list of objects, 
        decode it, and convert objects
        into a pandas DataFrame.
        
        Parameters
        ----------
        list_of_objects : list
            a list containing objects. 
            Each object in the list should have a `read()` method that returns raw data in
            bytes, which can be decoded using UTF-8 encoding.
        
        Returns
        -------
            A DataFrame object created from the raw data extracted from the list of objects.
            combined_df : DataFrame 
                Returned if the number of objects is greater than 1 
            df : DataFrame 
                Returned if there is only one object in the list of objects 
        '''
        # Get the number of elements inside the list_of_objects 
        number_of_objects = len(list_of_objects)
        # Conditional to check if the number_of_objects is greater than 1
        if number_of_objects > 1:
            for index, element in enumerate(list_of_objects):
                raw_data = element['Body'].read().decode('utf-8')
                df = pd.read_csv(StringIO(raw_data), delimiter=',')
                list_of_objects[index] = df 
            combined_df = pd.concat(list_of_objects)
            # Reset the index and drop the extra index column 
            combined_df.reset_index(inplace=True)
            combined_df.drop(columns='index', inplace=True)
            return combined_df 
        # In other cases, read in the single object. 
        else:
            for element in list_of_objects:
                raw_data = element.read().decode('utf-8')

            df = pd.read_csv(StringIO(raw_data), delimiter=',')

            return df 
         

    def build_dimension_table(self, df : pd.DataFrame, unique_column_name : str, order_of_columns : list):
        '''
        Creates a dimension table from a DataFrame using a specified
        unique column and order of columns.
        
        Parameters
        ----------
        df : pd.DataFrame
            A pandas DataFrame that contains the data from which the dimension table is built. 

        unique_column_name : str
            A string represnting the name of the column in the DataFrame (`df`) that contains the values 
            from which the dimension table is created. 
    
        order_of_columns : list
            A list which specifies the desired order of columns in the
            resulting dimension table. It determines the sequence in which columns will appear in the final
            DataFrame.
        
        Returns
        -------
            A pandas DataFrame that represents a dimension table.
        
        '''
        # Create a list of unique values using a column from the dataframe. 
        list_of_unique_names = list(df[unique_column_name].unique())
        print(list_of_unique_names)

        dimension_table_df = pd.DataFrame({unique_column_name: list_of_unique_names})

        # Setting the row_indexer as the index of the new dataframe
        row_indexer = dimension_table_df.index
        # Setting the column_indexer as the list containing the unique_column_name
        col_indexer = [f"{unique_column_name}_id"]
        # Setting the id values using the .loc method  
        dimension_table_df.loc[row_indexer, col_indexer] = dimension_table_df.index.to_numpy() + 1
        # Converting the id column inside the dimension table to an integer
        dimension_table_df[f"{unique_column_name}_id"] = dimension_table_df[f"{unique_column_name}_id"].astype(int)
        
        # Setting the column order ]
        column_order = order_of_columns
        dimension_table_df = dimension_table_df[column_order]
        return dimension_table_df

    def build_time_dimension_table(self, df : pd.DataFrame, datetime_field_column_name : str, column_order : list):
        '''
        
        Method to take a DataFrame, extracts various date and time
        components from a specified datetime column, adds additional columns with boolean values, generates
        a UUID for each row, and reorders the columns based on a specified order.
        
        Parameters
        ----------
        df : pd.DataFrame
            A pandas DataFrame containing the data to build a time
            dimension table.
        datetime_field_column_name : str
            A string representing the name of the column in the DataFrame `df` that
            contains datetime values which will be used to build the time dimension table.
        column_order : list
            A list which specifies the desired order of columns in the resulting
            DataFrame after building the time dimension table. 
    
        Returns
        -------
            A pandas DataFrame that represents a time dimension table.
        '''
        # Convert the date_extracted column to a datetime 
        df[datetime_field_column_name] = pd.to_datetime(df[datetime_field_column_name])

        # Extracting the year, month, day and timestamp from the date column 
        df['year'] = df[datetime_field_column_name].dt.year
        df['month'] = df[datetime_field_column_name].dt.month
        df['day'] = df[datetime_field_column_name].dt.day
        df['timestamp'] = df[datetime_field_column_name].dt.strftime('%H:%M:%S')
        df['date'] = df[datetime_field_column_name].dt.date

        # Extracting the quarter from the date column 
        df['quarter'] = df[datetime_field_column_name].dt.quarter
        df['day_of_week'] = df[datetime_field_column_name].dt.day_name()
        df['month_name'] = df[datetime_field_column_name].dt.month_name()

        # Boolean columns for month_end, leap_year, month_start, quarter_start and quarter_end
        df['is_month_end'] = df[datetime_field_column_name].dt.is_month_end
        df['is_leap_year'] = df[datetime_field_column_name].dt.is_leap_year
        df['is_month_start'] = df[datetime_field_column_name].dt.is_month_start
        df['is_quarter_start'] = df[datetime_field_column_name].dt.is_quarter_start
        df['is_quarter_end'] = df[datetime_field_column_name].dt.is_quarter_end

        df['date_uuid'] = [uuid4() for _ in range(len(df))]

        time_dimension_df_column_order = column_order
        # Assigning the order of columns
        # time_dimension_df_column_order = [
        #     'date_extracted_id', 'date_uuid', 'year', 'month', 'day',
        #         'date', 'timestamp', datetime_field_column_name,'quarter', 'day_of_week',
        #     'month_name', 'is_month_end', 'is_leap_year', 'is_month_start',
        #     'is_quarter_start', 'is_quarter_end'
        # ]
        # Applying the column order to the time_dimension dataframe
        df = df[time_dimension_df_column_order]

        return df 
    
    def build_fact_table(self, 
                         df : pd.DataFrame, 
                         job_title_df : pd.DataFrame, 
                         company_df : pd.DataFrame, 
                         location_df : pd.DataFrame, 
                         job_url_df : pd.DataFrame, 
                         description_df : pd.DataFrame, 
                         time_dimension_df : pd.DataFrame, 
                         website_df : pd.DataFrame
                         ):
        
        '''
        Merges data from multiple DataFrames to create a fact table for job
        data, including extracting salary information and applying static methods.
        
        Parameters
        ----------
        df : pd.DataFrame
            Takes in several DataFrames as input parameters to build a fact
            table. Below is a brief explanation of each parameter:
        job_title_df : pd.DataFrame
            The `job_title_df` parameter is a DataFrame containing information about job titles. This DataFrame
            includes columns such as `job_title` and corresponding identifiers or codes for each job
            title. This information is used to merge with the main DataFrame (`df`) to enrich the data with
            additional details related to job titles
        company_df : pd.DataFrame
            The `company_df` parameter in the `build_fact_table` function is a DataFrame containing information
            about companies. This DataFrame is used to merge with the main DataFrame (`df`) based on the
            `company_name` column to enrich the fact table with additional company information. The merging
            process helps consolidate data from different company information
        location_df : pd.DataFrame
            The `location_df` parameter in the `build_fact_table` function is a DataFrame containing
            information about job locations. This DataFrame is used to merge location data with the main
            DataFrame `df` during the process of building a fact table for job data. The merging process
            involves matching location information. 
        job_url_df : pd.DataFrame
            The `job_url_df` parameter in the `build_fact_table` function is a DataFrame containing information
            related to job URLs. This DataFrame is used to merge with the main DataFrame (`df`) to enrich the
            fact table with job URL details. The merging process is done based on the 'job_url'
        description_df : pd.DataFrame
            The `description_df` parameter in the `build_fact_table` function is a DataFrame containing job
            descriptions. This DataFrame is used to merge with other DataFrames such as `job_title_df`,
            `company_df`, `location_df`, `job_url_df`, and `time_dimension_df` to build the rest of the fact table
        time_dimension_df : pd.DataFrame
            The `time_dimension_df` parameter in the `build_fact_table` function is a DataFrame containing
            time-related information such as dates, timestamps, and other time dimensions that are relevant to
            the job data being processed. This DataFrame is used to merge with the main DataFrame `df` to create
            a fact table
        
        Returns
        -------
            The `build_fact_table` method returns a pandas DataFrame `fact_job_data_df` that contains
            information from various DataFrames merged together and processed. The DataFrame includes columns
            such as unique_id, date_uuid, job_title_id, company_name_id, location_id, job_url_id,
            job_description_id, date_extracted_id, salary_range, min_salary, max_salary, full_time_flag,
            contract_flag
        
        '''
        df['date_extracted'] = pd.to_datetime(df['date_extracted'])
        # Merging job_title_df to the source_df 
        title_merge_df = pd.merge(df, job_title_df, how='left', left_on='job_title', right_on='job_title')

        company_merge_df = pd.merge(title_merge_df, company_df, how='left', left_on='company_name', right_on='company_name')

        location_merge_df = pd.merge(company_merge_df, location_df, how='left', left_on='location', right_on='location')

        job_url_merged_df = pd.merge(location_merge_df, job_url_df, how='left', left_on='job_url', right_on='job_url')

        description_merged_df = pd.merge(job_url_merged_df, description_df, how='left', left_on='job_description', right_on='job_description')

        # Converting 'date_extracted' to datetime type
        description_merged_df['date_extracted'] = pd.to_datetime(description_merged_df['date_extracted'])
        time_dimension_df['date_extracted'] = pd.to_datetime(time_dimension_df['date_extracted'])
        
        print(description_merged_df.info())
        print(time_dimension_df.info())
        website_merged_df = pd.merge(description_merged_df, website_df, on='website_name', how='left')

        fact_job_data_df = pd.merge(website_merged_df, time_dimension_df, on='date_extracted', how='left')

        # Applying staticmethods to the dataframe 
        fact_job_data_df['min_salary'] = fact_job_data_df['salary_range'].apply(lambda x: self.extract_min_salary(x))
        fact_job_data_df['max_salary'] = fact_job_data_df['salary_range'].apply(lambda x: self.extract_max_salary(x))
        fact_job_data_df['full_time_flag'] = fact_job_data_df['salary_range'].apply(lambda x: self.is_full_time(x))
        fact_job_data_df['contract_flag'] = fact_job_data_df['salary_range'].apply(lambda x: self.is_contract(x))
        fact_job_data_df['competitive_flag'] = fact_job_data_df['salary_range'].apply(lambda x: self.is_competitive(x))

        # Adding uuid column to fact table to act as primary key
        fact_job_data_df['unique_id'] = [str(uuid4()) for _ in range(len(fact_job_data_df))]

        # Selecting and assigning the column_order 
        fact_job_data_df_order = ['unique_id', 'date_uuid', 'job_title_id', 'company_name_id',
       'location_id', 'job_url_id', 'job_description_id', 'date_extracted_id', 'website_name_id', 'salary_range',
       'min_salary', 'max_salary', 'full_time_flag', 'contract_flag',
       'competitive_flag'
            ]

        fact_job_data_df = fact_job_data_df[fact_job_data_df_order]

        return fact_job_data_df
    
    @staticmethod
    def get_geo_co_ordinates(location : str):
        '''
        Takes a location string as input and returns its latitude and
        longitude coordinates using the Nominatim geocoding service.
        
        Parameters
        ----------
        location : str
            A string representing the location. 

            The Nominatim geocoding service is used to retrieve the latitude and longitude coordinates of the provided
            location. 

        Returns
        -------
            A tuple containing the latitude and longitude of the given location. 
            If the location is not found, it returns a tuple with None values for latitude and
            longitude.

        '''
        geolocator = Nominatim(user_agent='location')
        location = geolocator.geocode(location)
        if location:
            return location.latitude, location.longitude
        else:
            return None, None
    

    @staticmethod
    def extract_from_url(url : str):
        '''
        Extracts the middle part of a URL using a regular expression
        pattern.
        
        Parameters
        ----------
        url : str
            The url of the website
        
        Returns
        -------
        extracted_text : str
            extracted text from the middle part of the URL using the regular expression pattern
            provided.
        
        '''

        pattern = r'(?<=\.)[^.]+(?=\.)'
        # use the following re pattern to extract the middle part of the url
        result = re.search(pattern, url)
        print(result)

        if result:
            extracted_text = result.group(0)
            print(extracted_text)

        return extracted_text
    
    @staticmethod
    def extract_min_salary(salary):
        '''
        Parses salary information in various formats and returns the
        minimum salary amount.
        
        Parameters
        ----------
        salary
            A string representing the salary formats commonly found in job listings. 
            It uses regular expressions 
            to match patterns like "£43,000 - £50,000 a year", "From £48,000 a year", "£75,000
        
        Returns
        -------
            The `extract_min_salary` function is designed to extract the minimum salary amount from various
            salary formats commonly found in job postings. It uses regular expressions to match patterns
            such as "£43,000 - £50,000 a year", "From £48,000 a year", "£75,000 a year", "£12.80 - £15.30 an
            hour", and "£
        
        '''
        if pd.isna(salary):
            return None
        # Case: "£43,000 - £50,000 a year" or "£41,587.93 - £97,961.67 a year"
        match = re.match(r'^\£([0-9,\.]+) - \£[0-9,\.]+ a year$', salary)
        if match:
            return float(match.group(1).replace(',', ''))
        # Case: "From £48,000 a year"
        match = re.match(r'^From \£([0-9,\.]+) a year$', salary)
        if match:
            return float(match.group(1).replace(',', ''))
        # Case: "£75,000 a year"
        match = re.match(r'^\£([0-9,\.]+) a year$', salary)
        if match:
            return float(match.group(1).replace(',', ''))
        # Case: "£12.80 - £15.30 an hour"
        match = re.match(r'^\£([0-9,\.]+) - \£[0-9,\.]+ an hour$', salary)
        if match:
            return float(match.group(1).replace(',', ''))
        # Case: "£600 - £635 a day"
        match = re.match(r'^\£([0-9,]+) - \£[0-9,]+ a day$', salary)
        if match:
            return float(match.group(1).replace(',', ''))
        return None

  
    @staticmethod
    def extract_max_salary(salary):
        """
        The function `extract_max_salary` in Python extracts the maximum salary value from various salary
        formats specified in the input string.
        
        Parameters
        ----------
        salary
            The `extract_max_salary` method is designed to extract the maximum salary value from various salary
        formats. It uses regular expressions to match different patterns in the salary string and extract
        the relevant numerical value.
        
        Returns
        -------
            The `extract_max_salary` method returns the maximum salary amount extracted from the input salary
        string. If the input salary is in a specific format like "£43,000 - £50,000 a year", "Up to £60,000
        a year", "£31 an hour", "£228.29 a day", "£12.80 - £15.30 an hour", or
        
        """
        if pd.isna(salary):
            return None
        # Case: "£43,000 - £50,000 a year" or "£41,587.93 - £97,961.67 a year"
        match = re.match(r'^\£[0-9,\.]+ - \£([0-9,\.]+) a year$', salary)
        if match:
            return float(match.group(1).replace(',', ''))
        # Case: "Up to £60,000 a year"
        match = re.match(r'^Up to \£([0-9,]+) a year$', salary)
        if match:
            return float(match.group(1).replace(',', ''))
        # Case: "£31 an hour"
        match = re.match(r'^\£([0-9,\.]+) an hour$', salary)
        if match:
            return float(match.group(1).replace(',', ''))
        # Case: "£228.29 a day"
        match = re.match(r'^\£([0-9,\.]+) a day$', salary)
        if match:
            return float(match.group(1).replace(',', ''))
        # Case: "£12.80 - £15.30 an hour"
        match = re.match(r'^\£[0-9,\.]+ - \£([0-9,\.]+) an hour$', salary)
        if match:
            return float(match.group(1).replace(',', ''))
        # Case: "£600 - £635 a day"
        match = re.match(r'^\£[0-9,\.]+ - \£([0-9,\.]+) a day$', salary)
        if match:
            return float(match.group(1).replace(',', ''))
        return None
 
    @staticmethod
    def is_full_time(salary):
        '''
        Checks if a given salary string indicates a full-time position based on
        specific keywords.
        
        Parameters
        ----------
        salary
            A string representing a salary range
        
        Returns
        -------
            A boolean value based on whether the input `salary` contains
            any of the specified keywords indicating a full-time position. 

            If the `salary` is NaN or does not
            contain any of the keywords, it returns False. 

            If the `salary` contains any of the specified
            keywords, it returns True.
        
        '''
        if pd.isna(salary):
            return False
        if any(keyword in salary for keyword in ['£', 'permanent', 'full-time', 'Up to £', 'Permanent', 'Full-time']):
            return True
        return False

    
    @staticmethod
    def is_contract(salary):
        '''
        Checks if a given salary string indicates a contract-based employment.
        Any keywords related to contract
        work such as 'contract', 'hour', 'day', 'Temporary', 'Temporary contract'.

        Parameters
        ----------
        salary
            A string which represents a salary range 
        
        Returns
        -------
            True, indicating that the salary is for a
            contract position. Otherwise, it returns False.
        
        '''
        if pd.isna(salary):
            return False
        if any(keyword in salary for keyword in ['contract', 'hour', 'day', 'Temporary', 'Temporary contract']):
            return True
        return False

    
    @staticmethod
    def is_competitive(salary):
        '''
        Checks if a salary is competitive. If the salary is `NaN` (not a
        number) or if it is equal to the string `'N/A'`

        Parameters
        ----------
        salary
            A string representing a salary
        
        Returns
        -------
            The function `is_competitive` is checking if the `salary` is either `NaN` or equal to the string
        `'N/A'`. If the `salary` meets either of these conditions, the function 
        
        returns `True` if the `salary` is either `NaN` or equal to the string
        `'N/A'`., indicating that the salary is considered competitive. 
        
        Otherwise, it returns `False`, indicating that the salary is not competitive.
        
        '''
        if pd.isna(salary) or salary == 'N/A':
            return True
        return False



