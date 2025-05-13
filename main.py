from datetime import date

import streamlit as st
import pandas as pd

import db
import utils


def main():
    st.title("Tippie Career Services Job Sourcer")
    
    with st.form("my_form"):
        # Load initial form data
        data = db.github_read()
        
        locations_str = ", ".join(data["locations"])
        finance_jobs_str = ", ".join(data["finance_jobs"])
        bais_jobs_str = ", ".join(data["bais_jobs"])
        accounting_jobs_str = ", ".join(data["accounting_jobs"])
        exclusion_keywords_str = ", ".join(data["exclusion_keywords"])
        
        
        days_old = st.slider("Days Old", 1, 90)
        num_jobs = st.slider("Number of jobs per category", 1, 50)
        locations_input = st.text_area('Locations: (comma seperated)', locations_str)
        finance_jobs_input = st.text_area('Finance Jobs: (comma seperated)', finance_jobs_str)
        bais_jobs_input = st.text_area('BAIS Jobs: (comma seperated)', bais_jobs_str)
        accounting_jobs_input = st.text_area('Accounting Jobs: (comma seperated)', accounting_jobs_str)
        exclusion_keywords_input = st.text_area('Keywords to Exclude: (comma seperated)', exclusion_keywords_str)
        
        gpp_csv = st.file_uploader("Upload Handshake Data", type=["csv"])
        
        submitted = st.form_submit_button("Submit")
        
    if submitted:
        locations = [loc.strip() for loc in locations_input.split(",")]
        finance_jobs = [job.strip() for job in finance_jobs_input.split(",")]
        bais_jobs = [job.strip() for job in bais_jobs_input.split(",")]
        accounting_jobs = [job.strip() for job in accounting_jobs_input.split(",")]
        exclusion_keywords = [key.strip() for key in exclusion_keywords_input.split(",")]
        
        data = {
            "locations": locations,
            "finance_jobs": finance_jobs,
            "bais_jobs": bais_jobs,
            "accounting_jobs": accounting_jobs,
            "exclusion_keywords": exclusion_keywords
        }
        db.github_write(data)
        
        job_dfs: dict[str, pd.DataFrame] = utils.get_jobs(
            job_titles={
                "finance": finance_jobs,
                "bais": bais_jobs,
                "accounting": accounting_jobs
            },
            locations=locations,
            days_old=days_old,
            num_jobs_wanted=num_jobs,
            exclusion_keywords=exclusion_keywords
        )
        
        jobs = pd.concat([job_dfs["finance_df"], job_dfs["bais_df"], job_dfs["accounting_df"]], ignore_index=True)
        jobs = jobs.drop_duplicates().reset_index(drop=True)
        
        st.dataframe(jobs)
        
        if gpp_csv:
            gpp_df = pd.read_csv(gpp_csv)
            
            international_jobs = utils.international_jobs(gpp_df, (finance_jobs + bais_jobs + accounting_jobs))
        
            excel_data = utils.to_excel(jobs, international_jobs)
            
        else:
            excel_data = utils.to_excel(jobs)
        
        st.download_button(
            label="Download Excel file",
            data=excel_data,
            file_name=f"results_{date.today().strftime("%Y-%m-%d")}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )


if __name__ == "__main__":
    main()
