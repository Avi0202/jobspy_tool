import pandas as pd
from jobspy import scrape_jobs


def jobspy_scraper(
    search_term: str = "Software Engineer",
    location: str = "San Francisco, CA",
    site_name: list = None,
    country_indeed: str = "USA",
    results_wanted: int = 20,
    hours_old: int = 72,
    distance: int = 50,
    offset: int = 0,
    job_type: str = "",
    is_remote: bool | None = None,
    easy_apply: bool | None = None,
    google_search_term: str = None,
    linkedin_fetch_description: bool = False,
    enforce_annual_salary: bool = False,
    verbose: int = 2,
    **kwargs
):
    """
    Scrape jobs from multiple job boards using JobSpy and return results as JSON.

    Parameters
    ----------
    search_term : str
        Main keyword for job search (e.g. "Software Engineer").
    location : str
        Location for the search (e.g. "New York, NY").
    site_name : list[str]
        List of job sites to scrape (e.g. ["linkedin", "indeed", "google"]).
    country_indeed : str
        Country code for Indeed or Glassdoor searches.
    results_wanted : int
        Number of job results desired.
    hours_old : int
        Filter by how recent the job posting is (in hours).
    distance : int
        Search radius in miles.
    offset : int
        Skip the first N results.
    job_type : str
        Job type filter (e.g. "fulltime", "internship").
    is_remote : bool
        Whether to only include remote jobs.
    easy_apply : bool
        Filter for easy-apply jobs (LinkedIn/Indeed).
    google_search_term : str
        Special query for Google Jobs (optional).
    linkedin_fetch_description : bool
        Fetch full LinkedIn job descriptions (slower).
    enforce_annual_salary : bool
        Convert pay to annual salary.
    verbose : int
        Verbosity level (0, 1, 2).

    Returns
    -------
    dict
        JSON-compatible dictionary with job results and metadata.
        Example:
        {
            "success": True,
            "count": 25,
            "data": [ {...}, {...}, ... ]
        }
    """

    try:
        # Default sites
        if site_name is None:
            site_name = ["indeed", "linkedin", "zip_recruiter", "google"]

        # Prepare params for JobSpy
        params = {
            "site_name": site_name,
            "search_term": search_term,
            "google_search_term": google_search_term or search_term,
            "location": location,
            "country_indeed": country_indeed,
            "results_wanted": results_wanted,
            "hours_old": hours_old,
            "distance": distance,
            "offset": offset,
            "verbose": verbose,
            "enforce_annual_salary": enforce_annual_salary,
            "linkedin_fetch_description": linkedin_fetch_description,
        }

        # Optional filters
        if job_type:
            params["job_type"] = job_type
        if is_remote is not None:
            params["is_remote"] = is_remote
        if easy_apply is not None:
            params["easy_apply"] = easy_apply

        # Merge any extra kwargs
        params.update(kwargs)

        # Scrape jobs
        jobs_df = scrape_jobs(**params)

        if jobs_df.empty:
            return {
                "success": True,
                "count": 0,
                "data": [],
                "message": "No jobs found. Try adjusting your filters."
            }

        # Convert DataFrame to JSON
        data_json = jobs_df.to_dict(orient="records")

        return {
            "success": True,
            "count": len(jobs_df),
            "data": data_json,
            "metadata": {
                "unique_companies": int(jobs_df["company"].nunique()) if "company" in jobs_df.columns else None,
                "top_locations": (
                    jobs_df["city"].value_counts().head(10).to_dict()
                    if "city" in jobs_df.columns else {}
                ),
            },
        }

    except Exception as e:
        return {
            "success": False,
            "error": str(e),
        }

