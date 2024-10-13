CREATE OR REPLACE VIEW public.cloud_engineering_jobs
 AS
 SELECT DISTINCT dj.job_title,
    dc.company_name,
    dl.location,
    f.salary_range,
    dd.date,
    dw.website_name,
    dju.job_url
   FROM fact_job_data f
     JOIN dim_job_title dj ON f.job_title_id = dj.job_title_id
     JOIN dim_company dc ON f.company_name_id = dc.company_name_id
     JOIN dim_location dl ON f.location_id = dl.location_id
     JOIN dim_job_url dju ON f.job_url_id = dju.job_url_id
     JOIN dim_date dd ON f.date_extracted_id = dd.date_extracted_id
     JOIN dim_website dw ON f.website_name_id = dw.website_name_id
  WHERE dj.job_title::text ~~ '%Cloud%'::text
  ORDER BY dd.date;


CREATE OR REPLACE VIEW public.data_analyst_jobs
 AS
 SELECT DISTINCT dj.job_title,
    dc.company_name,
    dl.location,
    f.salary_range,
    dd.date,
    dw.website_name,
    dju.job_url
   FROM fact_job_data f
     JOIN dim_job_title dj ON f.job_title_id = dj.job_title_id
     JOIN dim_company dc ON f.company_name_id = dc.company_name_id
     JOIN dim_location dl ON f.location_id = dl.location_id
     JOIN dim_job_url dju ON f.job_url_id = dju.job_url_id
     JOIN dim_date dd ON f.date_extracted_id = dd.date_extracted_id
     JOIN dim_website dw ON f.website_name_id = dw.website_name_id
  WHERE dj.job_title::text ~~ '%Analyst%'::text
  ORDER BY dd.date;


CREATE OR REPLACE VIEW public.data_engineering_jobs
 AS
 SELECT DISTINCT dj.job_title,
    dc.company_name,
    dl.location,
    f.salary_range,
    dd.date,
    dw.website_name,
    dju.job_url
   FROM fact_job_data f
     JOIN dim_job_title dj ON f.job_title_id = dj.job_title_id
     JOIN dim_company dc ON f.company_name_id = dc.company_name_id
     JOIN dim_location dl ON f.location_id = dl.location_id
     JOIN dim_job_url dju ON f.job_url_id = dju.job_url_id
     JOIN dim_date dd ON f.date_extracted_id = dd.date_extracted_id
     JOIN dim_website dw ON f.website_name_id = dw.website_name_id
  WHERE dj.job_title::text ~~ '%Data Engineer%'::text
  ORDER BY dd.date;