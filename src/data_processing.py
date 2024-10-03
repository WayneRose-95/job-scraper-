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
        # Get the current datetime 
        current_date = datetime.now() 
        # Create the file_path
        directory_name = f'job-data/{website_name}/{current_date.year}/{current_date.month}/{current_date.day}/'
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
       

    def upload_file_to_s3(self):
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



