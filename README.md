# job-scraper-
Repository to extract jobs from various websites 

# Overview 
![image](https://github.com/user-attachments/assets/43712db2-89a5-4102-912f-963458d1b5cb)

The job scraper aims to centralise information from various job websites into a central database running on PostgreSQL. 

Currently the process only handles `indeed` , but other websites are in the works. 

# Technologies Used 
 <a href="https://aws.amazon.com" target="_blank" rel="noreferrer"> <img src="https://raw.githubusercontent.com/devicons/devicon/master/icons/amazonwebservices/amazonwebservices-original-wordmark.svg" alt="aws" width="40" height="40"/> </a> 
<a href="https://www.python.org" target="_blank" rel="noreferrer"> <img src="https://raw.githubusercontent.com/devicons/devicon/master/icons/python/python-original.svg" alt="python" width="40" height="40"/> </a> 
<a href="https://www.selenium.dev" target="_blank" rel="noreferrer"> <img src="https://raw.githubusercontent.com/detain/svg-logos/780f25886640cef088af994181646db2f6b1a3f8/svg/selenium-logo.svg" alt="selenium" width="40" height="40"/> </a>
<a href="https://www.postgresql.org" target="_blank" rel="noreferrer"> <img src="https://raw.githubusercontent.com/devicons/devicon/master/icons/postgresql/postgresql-original-wordmark.svg" alt="postgresql" width="40" height="40"/> </a>
<a href="https://pandas.pydata.org/" target="_blank" rel="noreferrer"> <img src="https://raw.githubusercontent.com/devicons/devicon/2ae2a900d2f041da66e950e4d48052658d850630/icons/pandas/pandas-original.svg" alt="pandas" width="40" height="40"/> </a>
<a href="https://geopy.readthedocs.io/en/stable/" target="_blank" rel="noreferrer">
<img src="https://github.com/user-attachments/assets/98ee6174-bcbc-4832-91b1-6a2690d83db9" alt="GeoPy" width="40" height="40"/> </a>

# Prerequisites

- Anaconda3 or MiniConda3 or a Virtual Environment (venv) 
- AWS CLI
- An AWS IAM user with S3 Read and Write Permissions

# Usage 

1. Clone the repository by running the following command
  ```
  git clone https://github.com/WayneRose-95/job-scraper-
  ```
2. Navigate to the `job-scraper-` directory

 ```
 cd job-scraper-
 ```
3. Inside the job_scraper directory run the following conda command to create the `job_scraper` environment and install all dependencies
 ```
 conda env create -f environment.yaml
 ```
4. Populate the configuration files with the desired job titles and database

5. Once complete run
```
python main.py 
```
  To execute the main process

# Features 

Customize what job titles are needed, and how many pages are needed per website. 

Execute SQL statements on the database from Python 

Supports repeated ETL processes inside the database. 

For more information on how this is done, check the Wiki page (currently under development) 

# Current Data Model 

![image](https://github.com/user-attachments/assets/b8dc2689-ad40-4e9b-ba33-9c5fa45ff856)

# Wiki 
Currently in development 

# Future Improvements 

- Ingestion of websites, Reed, CV-Library and TotalJobs
- Implementation of Test Suite for both Unit and Integration Testing
- Collection of Company Information via separate process 
- Implementation of Dashboard to visualize job data
- Utilizing Infrastructure as Code to automate the creation of the S3 Bucket 
