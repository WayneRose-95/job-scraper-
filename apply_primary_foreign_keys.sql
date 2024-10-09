-- Script to Apply Primary and Foreign Keys to Database 

ALTER TABLE dim_company 
ADD PRIMARY KEY(company_name_id);

ALTER TABLE dim_date 
ADD PRIMARY KEY(date_uuid); 

ALTER TABLE dim_job_title 
ADD PRIMARY KEY(job_title_id); 

ALTER TABLE dim_job_url
ADD PRIMARY KEY(job_url_id); 

ALTER TABLE dim_location 
ADD PRIMARY KEY(location_id); 

ALTER TABLE dim_description
ADD PRIMARY KEY(job_description_id);

ALTER TABLE fact_job_data 
ADD PRIMARY KEY(unique_id);  



ALTER TABLE fact_job_data
ADD CONSTRAINT FK_dim_company 
FOREIGN KEY (company_name_id) REFERENCES dim_company(company_name_id);

ALTER TABLE fact_job_data
ADD CONSTRAINT FK_dim_job_title
FOREIGN KEY (job_title_id) REFERENCES dim_job_title(job_title_id);

ALTER TABLE fact_job_data
ADD CONSTRAINT FK_dim_job_url
FOREIGN KEY (job_url_id) REFERENCES dim_job_url(job_url_id);

ALTER TABLE fact_job_data
ADD CONSTRAINT FK_dim_location 
FOREIGN KEY (location_id) REFERENCES dim_location(location_id);

ALTER TABLE fact_job_data 
ADD CONSTRAINT FK_date_uuid 
FOREIGN KEY(date_uuid) REFERENCES dim_date(date_uuid); 

ALTER TABLE fact_job_data 
ADD CONSTRAINT FK_job_description_id 
FOREIGN KEY(job_description_id) REFERENCES dim_description(job_description_id);


