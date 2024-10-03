import boto3
from botocore.exceptions import ClientError
from datetime import datetime
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
        pass

class DataFrameManipulation: 

    def raw_to_dataframe(self):
        pass 

    def build_dimension_table(self):
        pass 

    def build_time_dimension_table(self):
        pass 

    def build_fact_table(self):
        pass 

    @staticmethod
    def extract_from_url():
        pass 

    @staticmethod
    def extract_min_salary(salary):
        pass 
    
    @staticmethod
    def extract_max_salary(salary):
        pass 

    @staticmethod
    def is_full_time(salary):
        pass 

    @staticmethod
    def is_contract(salary):
        pass 
    
    @staticmethod
    def is_competitive(salary):
        pass 



