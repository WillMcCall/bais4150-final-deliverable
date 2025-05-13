import time
import re
from io import BytesIO

import pandas as pd
from jobspy import scrape_jobs
import streamlit as st


business_analytics_companies = (
    "JP Morgan Chase",
    "Associated Wholesale Grocers",
    "Deloitte",
    "Dover Corporation",
    "Energy Solutions and Supplies LLC",
    "EY",
    "Farm Bureau Financial Services",
    "Fastenal",
    "Foresight Strategy",
    "Infosys Consulting",
    "Intelsat",
    "Linn County Rural Electric",
    "PMG",
    "Principal Financial",
    "ProShares",
    "RSM",
    "State Farm",
    "Texas Rangers Baseball Club",
    "Walker's Homestead",
    "Principal Financial Group",
    "Collins Aerospace",
    "HNI",
    "University of Iowa",
    "Wellmark Blue Cross Blue Shield",
    "Aegon Asset Management",
    "BMO Harris",
    "Dish Network",
    "Fetch Rewards",
    "John Deere",
    "Transamerica",
    "Wells Fargo",
    "Abercrombie & Fitch",
    "Affiliated Monitoring",
    "Aflac Global Investments",
    "Air Methods",
    "Allegiant Air",
    "Alliant",
    "American Bank and Trust",
    "American Bear Logistics",
    "Athene",
    "Bank of New York Mellon",
    "Bickford Senior Living",
    "Bloomberg LP",
    "Brandes Investment Partners",
    "Broadlawns Medical Center",
    "Brown Gibbons Lang",
    "Buildertrend",
    "Bureau of Land Management Denver",
    "Daikin Applied America",
    "Delta Dental",
    "Department of Defense",
    "DLL",
    "DRW Holdings",
    "EDGE 10 Group",
    "Evolytics",
    "Federal Reserve Bank of Kansas City",
    "First National Bank of Omaha",
    "Frontier Co-Op",
    "Goldman Sachs",
    "Growers Edge",
    "G-Squared Partners",
    "Gypsum Consulting",
    "Hirschbach Motor lines",
    "IDT",
    "Knapheide Manufacturing",
    "Kyocera",
    "Minsheng Bank",
    "Modern Woodmen of America",
    "MScience",
    "Nike",
    "Northern Oil & Gas, Inc.",
    "Northern Trust",
    "Oneida Indian Nation",
    "Pearson",
    "PetSmart",
    "Philadelphia Phillies",
    "Prudential Financial",
    "PwC",
    "Quantiphi",
    "Republic National Distributing Company",
    "Rollins",
    "San Francisco Giants",
    "SMG",
    "Standford Health",
    "Stifel",
    "Success Bank",
    "Temenos",
    "Thrivent",
    "TS Imagine Financial Technology",
    "UIHC",
    "United States Air Force",
    "University of Southern California",
    "US Army",
    "US Bank",
    "Walmart",
    "West Liberty Foods",
    "Xorbix Technologies, Inc.",
    "Zeitgeist Research",
    "Zijin Mining"
)

accounting_companies = (
    "PriceWaterhouseCoopers",
    "RSM",
    "Deloitte",
    "EY",
    "KPMG",
    "Athene",
    "CliftonLarsonAllen",
    "King Reinsch Prosser & Co LLP",
    "KUHL, Phillips & Jans Inc",
    "Mrjenovich & Bertucci, Ltd",
    "Plante Moran",
    "Savant",
    "Srameck & Hightowers, CPAs",
    "AndersonWeber Toyota",
    "Barclays",
    "BDO",
    "BerganKDV",
    "BKD",
    "Crowe",
    "Eder Casella & Co",
    "Evercore",
    "FASB",
    "Goldman Sachs",
    "IMS Branded Sol",
    "King, Reinsch, Prosser & Co",  # Keeping both King Reinsch entries in case they're different offices/entities
    "Koppenhaver & Associates CPA",
    "Lumen Technologies",
    "Metro Pavers",
    "Motorola Solutions",
    "PBMares LLP",
    "Pendulum  Holdings",
    "Pepsi",
    "Rockworth Companies",
    "Simmons Perrine",
    "Spotix, Inc.",
    "State Auditor",
    "Transamerica",
    "UIHC",
    "WipFli LLP"
)
finance_companies = (
    "Advent Health",
    "Aegon",
    "Alliant",
    "American Equity",
    "American Family Ventures",
    "Aflac",
    "Affiliated Monitoring",
    "Aegon Asset Management",
    "Bank of America",
    "Beekman Advisors",
    "BMO Harris",
    "BlackRock",
    "Bloomberg LP",
    "Blue Cross Blue Shield",
    "Boston Scientific",
    "Brandes Investment Partners",
    "Brown Gibbons Lang",
    "Carrier",
    "Casey's",
    "Caterpillar Financial",
    "CF Industries",
    "CIBC",
    "Collins Aerospace",
    "Commodities & Ingredients Hedging (CIH)",
    "CRST",
    "Crowe LLP",
    "CUNA Mutual Group",
    "Deloitte",
    "DRW Holdings",
    "DWS Group",
    "Equitable Advisors",
    "EY",
    "Farm Bureau Financial Services",
    "FedEx",
    "Federal Reserve Bank of Chicago",
    "First National Bank of Omaha",
    "Fiserv",
    "Frontier Co-Op",
    "Gess International",
    "GLC Advisors",
    "Goldman Sachs",
    "Grainger",
    "Grand Falls Casino & Golf",
    "Gypsum Consulting",
    "Hickory Hills Financial",
    "Hirschbach Motor lines",
    "HNI",
    "Hourglass Wealth Management",
    "IDX Technologies",
    "Integris Health",
    "InterOcean Capital",
    "JP Morgan Chase",
    "Kiewit Corp.",
    "KPMG",
    "Lincoln Internaional",
    "Madison Capital Funding",
    "Messina Group Consulting Solutions",
    "Metropolitan Commercial Bank",
    "Minsheng Bank",
    "MLB",
    "Modern Woodmen of America",
    "Morningstar",
    "Mscience",
    "Northern Oil & Gas, Inc.",
    "Pearson",
    "Performance Trust Capital Partners",
    "PetSmart",
    "PGIM",
    "Principal Financial Group",
    "ProShares",
    "Prudential Financial",
    "Quick Release",
    "Rally Appraisel",
    "RERC (Real Estate Research Corp)",
    "RSM",
    "SitusAMC",
    "Stifel",
    "Stout",
    "Success Bank",
    "Symmetry Financial",
    "The Mather Group",
    "The University of Iowa",
    "Thrivent",
    "Transamerica",
    "TS Imagine Financial Technology",
    "UICA",
    "Uline",
    "United Health Group",
    "United TechnologiesCOrporation",
    "UnityPoint Health Des Moines Foundation",
    "US Army",
    "VA Hospital",
    "VerifyYou Startup",
    "Walker's Homestead",
    "Wellmark Blue Cross Blue Shield",
    "Wells Fargo",
    "William Blair",
    "Wisconsin Badgers",
    "Xorbix Technologies, Inc.",
    "Zijin Mining",
    "Zurich Insurance"
)



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
    

def score_jobs(jobs: pd.DataFrame, num_jobs) -> pd.DataFrame:
    
    def score_job(job):
        # Clean fields
        title = str(job.get("title", "")).lower()
        desc  = str(job.get("description", "")).lower()
        text  = f"{title} {desc}"
        comp  = str(job.get("company", "")).lower()

        if re.search(r"\b\d+\s*[\+–-]?\s*\d*\s*(years|yrs)[’']?\s+(of\s+)?(relevant\s+)?experience", desc, flags=re.IGNORECASE):
            return -9999

        # — 2. Company match bonus
        ba_hit = any(c.lower() in title or c.lower() in comp for c in business_analytics_companies)
        ac_hit = any(c.lower() in title or c.lower() in comp for c in accounting_companies)
        fn_hit = any(c.lower() in title or c.lower() in comp for c in finance_companies)

        company_score = 100 if (
            ba_hit or 
            ac_hit or 
            fn_hit
        ) else 0

        # — 3. Education requirement bonus
        edu_patterns = [
            r"master[’']?s (degree|student|program|level)",  # e.g. master's degree, master’s student
            r"graduate (student|degree|level|candidate|program)",  # e.g. graduate degree, graduate-level role
            r"postgraduate",  # postgraduate degree or program
            r"\bmacc\b", r"\bmsba\b", r"\bmfin\b",  # program acronyms
            r"advanced (degree|education)",  # some use "advanced degree required"
            r"seeking (graduate|master[’']?s) students",  # targeting grads
            r"graduate school",  # related references
            r"master[’']?s preferred", r"graduate degree preferred",  # preferred mentions
            r"master[’']?s required", r"graduate degree required"  # explicit requirements
        ]
        education_score = 300 if any(re.search(pat, desc, flags=re.IGNORECASE) for pat in edu_patterns) else 0

        # — 4. Entry-level bonus
        entry_patterns = [
            r"recent graduate",
            r"graduating (this year|soon|ms student|master[’']?s student)?",
            r"upcoming graduate",
            r"(spring|summer|fall|winter) graduate",
            r"new grad(uate)?",
            r"graduate hiring",
            r"seeking (upcoming|graduating) graduates",
            r"entry[-\s]?level",
            r"junior analyst",
            r"early career",
            r"entry[-\s]?opportunity",
        ]
        entry_level_score = 100 if any(re.search(pat, desc, flags=re.IGNORECASE) for pat in entry_patterns) else 0

        # — 5. Exclusion penalty for higher-level roles
        exclusion_patterns = [
            r"\bsr\.?\b", r"\bsenior\b", r"\bmanager\b", r"\bdirector\b",
            r"\bvice[-\s]?president\b", r"\bvp\b", r"\bpresident\b", r"\bcfo\b",
            r"\bchief\b", r"\blead\b", r"\bhead\b", r"\bprincipal\b", r"\bexecutive\b",
            r"\bpartner\b", r"\bowner\b", r"\bfounder\b", r"\bofficer\b",
            r"senior analyst", r"lead analyst", r"financial controller",
            r"portfolio manager", r"investment manager", r"risk manager",
            r"senior accountant", r"audit manager", r"tax manager",
            r"finance manager", r"financial advisor", r"compliance officer",
            r"business manager", r"accounting supervisor", r"budget director",
            r"strategy consultant", r"engagement manager",
            r"data architect", r"analytics architect", r"solutions architect",
            r"senior consultant", r"principal consultant", r"erp consultant"
        ]
        exclusion_penalty = -9999 if any(re.search(pat, title, flags=re.IGNORECASE) for pat in exclusion_patterns) else 0

        # — 6. Salary cap bonus
        salary_score = 0
        try:
            min_amt = float(job.get("min_amount", 0))
            max_amt = float(job.get("max_amount", 0))
            if min_amt <= 100000 and max_amt <= 100000:
                salary_score = 15
        except:
            pass


        final_score = company_score + education_score + entry_level_score + exclusion_penalty + salary_score
        return final_score

    # Apply the score_job function to each job in the DataFrame and store the results in a new column 'score'
    jobs['score'] = jobs.apply(lambda job: score_job(job), axis=1)

    # Sort the jobs by score in descending order (highest scores first)
    jobs_sorted = jobs.sort_values(by='score', ascending=False)

    return jobs_sorted.head(num_jobs)



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
                # Ensure progress_counter doesn't go over 1 in edge cases
                if progress_counter > 1:
                    progress_counter = 1.0
                progress_bar.progress(progress_counter, text="Scraping in progress. Please wait...")
                time.sleep(5) # Avoid rate limiting
        
        # Combine and process all scraped jobs for this category
        df = pd.concat(dfs, ignore_index=True)
        cleaned_df = clean_jobs(df)
        final_df = score_jobs(cleaned_df, num_jobs_wanted)
        
        output_dictionary[f"{category}_df"] = final_df
    
    progress_bar.progress(1, text="Finishing up!")
    progress_bar.empty()
    
    return output_dictionary
    
    
def international_jobs(gpp_df, keywords, num_intern=2, num_fulltime=8):
    # Filter for international-friendly jobs (any of the 3 columns is "Yes")
    intl_cols = [
        "Student Qualifications Accepts CPT Candidates? (Yes / No)",
        "Student Qualifications Accepts OPT Candidates? (Yes / No)",
        "Student Qualifications Accepts OPT/CPT Candidates? (Yes / No)"
    ]
    eligible = gpp_df[gpp_df[intl_cols].eq("Yes").any(axis=1)]

    # Filter by keyword match in job title
    eligible = eligible[
        eligible["Jobs Title"].str.contains('|'.join(map(re.escape, keywords)), case=False, na=False)
    ]

    # Split by employment type
    internships = eligible[
        eligible["Employment Type Name"].str.contains("intern", case=False, na=False)
    ]
    fulltimes = eligible[
        eligible["Employment Type Name"].str.contains("full", case=False, na=False)
    ]

    # Random sample
    sample_intern = internships.sample(n=min(num_intern, len(internships)), replace=False, random_state=1)
    sample_full = fulltimes.sample(n=min(num_fulltime, len(fulltimes)), replace=False, random_state=1)

    return pd.concat([sample_intern, sample_full], ignore_index=True)

    
def to_excel(df, international_df=None):
        output = BytesIO()
        
        if international_df is None:
            with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                df.to_excel(writer, sheet_name="Domestic", index=False)
                
        else:            
            with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                df.to_excel(writer, sheet_name="Domestic", index=False)
                international_df.to_excel(writer, sheet_name="International", index=False)
                
        processed_data = output.getvalue()
        return processed_data
