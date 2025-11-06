import pandas as pd
from jobspy import scrape_jobs
from strands_agents.tools.toolkit import Toolkit
import logging


class JobSpyScraperTool(Toolkit):
    def __init__(self):
        super().__init__()

        self.logger = logging.getLogger("jobspy_scraper_tool")
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter("%(levelname)s | %(asctime)s | %(message)s")
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
            self.logger.setLevel(logging.INFO)

        self.register(
            self.scrape_job_listings,
            name="jobspy_scraper",
            description="Scrapes job listings from multiple sources using JobSpy and returns results as JSON."
        )

    async def scrape_job_listings(
        self,
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
        verbose: int = 2
    ):
        """
        Scrape jobs from multiple job boards using JobSpy and return results as JSON.
        """

        try:
            if site_name is None:
                site_name = ["indeed", "linkedin", "zip_recruiter", "google"]

            self.logger.info(f"🔍 Searching '{search_term}' jobs in {location} across {', '.join(site_name)}")

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

            if job_type:
                params["job_type"] = job_type
            if is_remote is not None:
                params["is_remote"] = is_remote
            if easy_apply is not None:
                params["easy_apply"] = easy_apply


            jobs_df = scrape_jobs(**params)

            if jobs_df.empty:
                self.logger.warning("No jobs found for given parameters.")
                return {
                    "success": True,
                    "count": 0,
                    "data": [],
                    "message": "No jobs found. Try adjusting your filters."
                }

            data_json = jobs_df.to_dict(orient="records")

            self.logger.info(f"✅ Found {len(jobs_df)} jobs for '{search_term}'")

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
            self.logger.error(f"❌ JobSpy scraping failed: {e}")
            return {"success": False, "error": str(e)}
