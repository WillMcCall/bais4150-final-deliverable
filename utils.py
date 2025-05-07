import time

import pandas as pd
from jobspy import scrape_jobs
import streamlit as st


def clean_jobs(jobs: pd.DataFrame) -> pd.DataFrame:
    # Removes Duplicates
    cleaned_jobs = jobs.drop_duplicates().reset_index(drop=True)
    
    # Removes all these unnecessary columns
    cleaned_jobs = cleaned_jobs.drop(
        ["id", "site", "job_url_direct", "job_type", "salary_source", "currency", "is_remote", "job_level", "job_function", 
        "listing_type", "emails", "company_industry", "company_logo", "company_url", "company_addresses", 
        "company_description", "skills", "experience_range", "company_rating", "company_reviews_count", "vacancy_count", 
        "work_from_home_type", "interval"], axis=1, errors='ignore')
    
    return cleaned_jobs
    
    
def filter_jobs(jobs: pd.DataFrame, num_jobs: int) -> pd.DataFrame:
    jobs = jobs[(jobs['min_amount'] >= 50000) & (jobs['max_amount'] <= 80000)] # Only keep jobs paying $50,000+, remove jobs paying $80,000+
    jobs = jobs.sort_values("min_amount", ascending=False)
    jobs = jobs.head(num_jobs // 3)
    
    return jobs


def get_jobs(
    job_titles: dict[str, list[str]],
    locations: list[str],
    days_old: int,
    num_jobs_wanted: int
) -> dict[str, pd.DataFrame]:
    
    sum_job_titles = sum(len(lst) for lst in job_titles.values())
    time_estimate_minutes = (len(locations) * sum_job_titles) / 60
    st.write(f"Time Estimate: {time_estimate_minutes:.1f} minutes")
    
    progress_bar = st.progress(0, text="Starting to look for jobs. Please wait...")
    progress_counter = 0
    
    output_dictionary = {}
    
    for category, titles in job_titles.items():
        dfs: list[pd.DataFrame] = []
        for job_title in titles:
            for location in locations:
                dfs.append(scrape_jobs(
                    site_name=["indeed", "linkedin", "zip_recruiter", "glassdoor"],
                    search_term=job_title,
                    location=location,
                    results_wanted=2,
                    hours_old=(days_old * 24),
                    country_indeed='USA',
                    verbose=1
                ))
                progress_counter += ((100 / sum_job_titles) / 100)
                progress_bar.progress(progress_counter, text="Scraping in progress. Please wait...")
                time.sleep(5) # Avoid rate limiting
        
        # Combine and process all scraped jobs for this category
        df = pd.concat(dfs, ignore_index=True)
        cleaned_df = clean_jobs(df)
        final_df = filter_jobs(cleaned_df, num_jobs_wanted)
        
        output_dictionary[f"{category}_df"] = final_df
    
    progress_bar.progress(1, text="Finishing up!")
    progress_bar.empty()
    
    return output_dictionary
