import requests
from django.core.management.base import BaseCommand
from medjobs.models import Job
from datetime import datetime
from bs4 import BeautifulSoup


class Command(BaseCommand):
    help = "Fetch jobs from SEHA, NMC, Burjeel, and Sheikh Khalifa Medical City APIs"

    def fetch_seha(self, limit=50, max_jobs=1000):
        self.stdout.write("Fetching SEHA jobs...")
        base_url = "https://fa-eutv-saasfaprod1.fa.ocs.oraclecloud.com/hcmRestApi/resources/latest/recruitingCEJobRequisitions"

        params = {
            "onlyData": "true",
            "expand": (
                "requisitionList.workLocation,"
                "requisitionList.otherWorkLocations,"
                "requisitionList.secondaryLocations,"
                "flexFieldsFacet.values,"
                "requisitionList.requisitionFlexFields"
            ),
            "finder": (
                "findReqs;siteNumber=CX_1,"
                "facetsList=LOCATIONS;WORK_LOCATIONS;WORKPLACE_TYPES;"
                "TITLES;CATEGORIES;ORGANIZATIONS;POSTING_DATES;FLEX_FIELDS,"
                "selectedPostingDatesFacet=30,"
                "selectedLocationsFacet=300000000446183,"  # UAE
                f"limit={limit},sortBy=POSTING_DATES_DESC"
            ),
        }

        count = 0
        for offset in range(0, max_jobs, limit):
            if count >= max_jobs:
                break
            params["offset"] = offset
            r = requests.get(base_url, params=params)

            if r.status_code != 200:
                self.stderr.write(f"SEHA API error {r.status_code}")
                break

            items = r.json().get("items", [])
            if not items:
                break

            for job in items[0].get("requisitionList", []):
                if count >= max_jobs:
                    break

                job_id = job.get("Id")
                job_url = f"https://fa-eutv-saasfaprod1.fa.ocs.oraclecloud.com/hcmUI/CandidateExperience/en/sites/CX_1/requisitions/preview/{job_id}"

                posted_date = None
                if job.get("PostedDate"):
                    try:
                        posted_date = datetime.strptime(job["PostedDate"], "%Y-%m-%d").date()
                    except:
                        pass

                Job.objects.update_or_create(
                    external_id=job["Id"], # ✅ now using Oracle ID
                    defaults={
                        "title": job.get("Title", ""),
                        "location": job.get("PrimaryLocation", ""),
                        "hospital_name": "SEHA",
                        "posted_date": posted_date,
                        "job_url": job_url,
                    }
                )
                count += 1

        self.stdout.write(self.style.SUCCESS(f"Imported/Updated {count} SEHA jobs"))
        return count

    def fetch_nmc(self, limit=50, max_jobs=1000):
        self.stdout.write("Fetching NMC jobs...")
        base_url = "https://eiby.fa.em2.oraclecloud.com/hcmRestApi/resources/latest/recruitingCEJobRequisitions"

        params = {
            "onlyData": "true",
            "expand": (
                "requisitionList.workLocation,"
                "requisitionList.otherWorkLocations,"
                "requisitionList.secondaryLocations,"
                "flexFieldsFacet.values,"
                "requisitionList.requisitionFlexFields"
            ),
            "finder": (
                "findReqs;siteNumber=CX_1,"
                "facetsList=LOCATIONS;WORK_LOCATIONS;WORKPLACE_TYPES;"
                "TITLES;CATEGORIES;ORGANIZATIONS;POSTING_DATES;FLEX_FIELDS,"
                "selectedPostingDatesFacet=30,"
                "selectedLocationsFacet=300000000289054,"  # UAE for NMC
                f"limit={limit},sortBy=POSTING_DATES_DESC"
            ),
        }

        count = 0
        for offset in range(0, max_jobs, limit):
            if count >= max_jobs:
                break
            params["offset"] = offset
            r = requests.get(base_url, params=params)

            if r.status_code != 200:
                self.stderr.write(f"NMC API error {r.status_code}")
                break

            items = r.json().get("items", [])
            if not items:
                break

            for job in items[0].get("requisitionList", []):
                if count >= max_jobs:
                    break

                job_id = job.get("Id")
                job_url = f"https://eiby.fa.em2.oraclecloud.com/hcmUI/CandidateExperience/en/sites/CX_1/requisitions/preview/{job_id}"

                posted_date = None
                if job.get("PostedDate"):
                    try:
                        posted_date = datetime.strptime(job["PostedDate"], "%Y-%m-%d").date()
                    except:
                        pass

                Job.objects.update_or_create(
                    external_id=job["Id"],  # ✅ unique by ID
                    defaults={
                        "title": job.get("Title", ""),
                        "location": job.get("PrimaryLocation", ""),
                        "hospital_name": "NMC",
                        "posted_date": posted_date,
                        "job_url": job_url,
                    }
                )
                count += 1

        self.stdout.write(self.style.SUCCESS(f"Imported/Updated {count} NMC jobs"))
        return count

    def fetch_burjeel(self, max_jobs=100):
        self.stdout.write("Fetching Burjeel jobs (scrape)...")
        url = "https://burjeel.com/careers/"
        r = requests.get(url)
        if r.status_code != 200:
            self.stderr.write("Burjeel page not reachable")
            return 0

        soup = BeautifulSoup(r.text, "html.parser")
        job_elems = soup.select(".current-openings .job-card")  # adjust selector if needed
        count = 0
        for job in job_elems:
            if count >= max_jobs:
                break

            title = job.select_one(".job-title").get_text(strip=True)
            location = job.select_one(".location").get_text(strip=True) if job.select_one(".location") else "UAE"
            posted_date = None
            date_elem = job.select_one(".date-posted")
            if date_elem:
                try:
                    posted_date = datetime.strptime(date_elem.get_text(strip=True), "%Y-%m-%d").date()
                except:
                    pass

            job_url_elem = job.select_one("a")
            job_url = job_url_elem["href"] if job_url_elem else url

            # Burjeel might not provide an ID → fallback: use URL as external_id
            Job.objects.update_or_create(
                external_id=job["Id"],  # ✅ fallback unique key
                defaults={
                    "title": title,
                    "location": location,
                    "hospital_name": "Burjeel",
                    "posted_date": posted_date,
                    "job_url": job_url,
                }
            )
            count += 1

        self.stdout.write(self.style.SUCCESS(f"Imported/Updated {count} Burjeel jobs (scrape)"))
        return count

    def fetch_skmc(self, limit=50, max_jobs=1000):
        self.stdout.write("Fetching SKMC jobs...")
        base_url = "https://fa-exqb-saasfaprod1.fa.ocs.oraclecloud.com/hcmRestApi/resources/latest/recruitingCEJobRequisitions"

        params = {
            "onlyData": "true",
            "expand": (
                "requisitionList.workLocation,"
                "requisitionList.otherWorkLocations,"
                "requisitionList.secondaryLocations,"
                "flexFieldsFacet.values,"
                "requisitionList.requisitionFlexFields"
            ),
            "finder": (
                "findReqs;siteNumber=CX_1003,"  # SKMC site number
                "facetsList=LOCATIONS;WORK_LOCATIONS;WORKPLACE_TYPES;"
                "TITLES;CATEGORIES;ORGANIZATIONS;POSTING_DATES;FLEX_FIELDS,"
                "selectedPostingDatesFacet=30,"
                f"limit={limit},sortBy=POSTING_DATES_DESC"
            ),
        }

        count = 0
        for offset in range(0, max_jobs, limit):
            if count >= max_jobs:
                break
            params["offset"] = offset
            r = requests.get(base_url, params=params)

            if r.status_code != 200:
                self.stderr.write(f"SKMC API error {r.status_code}")
                break

            items = r.json().get("items", [])
            if not items:
                break

            for job in items[0].get("requisitionList", []):
                if count >= max_jobs:
                    break

                job_id = job.get("Id")
                job_url = f"https://fa-exqb-saasfaprod1.fa.ocs.oraclecloud.com/hcmUI/CandidateExperience/en/sites/CX_1003/requisitions/preview/{job_id}"

                posted_date = None
                if job.get("PostedDate"):
                    try:
                        posted_date = datetime.strptime(job["PostedDate"], "%Y-%m-%d").date()
                    except:
                        pass

                Job.objects.update_or_create(
                    external_id=job["Id"],  # ✅ unique by Oracle ID
                    defaults={
                        "title": job.get("Title", ""),
                        "location": job.get("PrimaryLocation", ""),
                        "hospital_name": "Sheikh Khalifa Medical City",
                        "posted_date": posted_date,
                        "job_url": job_url,
                    }
                )
                count += 1

        self.stdout.write(self.style.SUCCESS(f"Imported/Updated {count} SKMC jobs"))
        return count

    def fetch_aster(self, limit=50, max_jobs=1000):
        self.stdout.write("Fetching Aster jobs...")
        base_url = "https://hcdtgccprod-iayeqy.fa.ocs.oraclecloud.com/hcmRestApi/resources/latest/recruitingCEJobRequisitions"

        params = {
            "onlyData": "true",
            "expand": (
                "requisitionList.workLocation,"
                "requisitionList.otherWorkLocations,"
                "requisitionList.secondaryLocations,"
                "flexFieldsFacet.values,"
                "requisitionList.requisitionFlexFields"
            ),
            "finder": (
                "findReqs;siteNumber=CX,"
                "facetsList=LOCATIONS;WORK_LOCATIONS;WORKPLACE_TYPES;"
                "TITLES;CATEGORIES;ORGANIZATIONS;POSTING_DATES;FLEX_FIELDS,"
                "selectedPostingDatesFacet=30,"
                "selectedLocationsFacet=300000000254942,"
                f"limit={limit},sortBy=POSTING_DATES_DESC"
            ),
        }

        count = 0
        for offset in range(0, 1000, limit):
            if count >= max_jobs:
                break
            params["offset"] = offset
            r = requests.get(base_url, params=params)

            if r.status_code != 200:
                self.stderr.write(f"Aster API error {r.status_code}")
                break

            items = r.json().get("items", [])
            if not items:
                break

            for job in items[0].get("requisitionList", []):
                if count >= max_jobs:
                    break

                job_url = f"https://hcdtgccprod-iayeqy.fa.ocs.oraclecloud.com/hcmUI/CandidateExperience/en/sites/CX/requisitions/preview/{job['Id']}"

                posted_date = None
                if job.get("PostedDate"):
                    try:
                        posted_date = datetime.strptime(job["PostedDate"], "%Y-%m-%d").date()
                    except:
                        pass

                Job.objects.update_or_create(
                    external_id=f"aster-{job['Id']}",
                    defaults={
                        "title": job.get("Title", ""),
                        "location": job.get("PrimaryLocation", ""),
                        "hospital_name": "Aster",
                        "posted_date": posted_date,
                        "job_url": job_url,
                    }
                )
                count += 1

        self.stdout.write(self.style.SUCCESS(f"Imported/Updated {count} Aster jobs"))
        return count

    def fetch_american_hospital(self, limit=50, max_jobs=500):
        """Fetch jobs from American Hospital Oracle HCM API"""
        self.stdout.write("Fetching American Hospital jobs...")
        base_url = "https://fa-epvs-saasfaprod1.fa.ocs.oraclecloud.com/hcmRestApi/resources/latest/recruitingCEJobRequisitions"

        params = {
            "onlyData": "true",
            "expand": (
                "requisitionList.workLocation,"
                "requisitionList.otherWorkLocations,"
                "requisitionList.secondaryLocations,"
                "flexFieldsFacet.values,"
                "requisitionList.requisitionFlexFields"
            ),
            "finder": (
                "findReqs;siteNumber=CX_1,"
                "facetsList=LOCATIONS;WORK_LOCATIONS;WORKPLACE_TYPES;"
                "TITLES;CATEGORIES;ORGANIZATIONS;POSTING_DATES;FLEX_FIELDS,"
                "selectedPostingDatesFacet=30,"
                # We may also need a selectedLocationsFacet for UAE if required
                f"limit={limit},sortBy=POSTING_DATES_DESC"
            ),
        }

        count = 0
        for offset in range(0, max_jobs, limit):
            if count >= max_jobs:
                break
            params["offset"] = offset
            r = requests.get(base_url, params=params)

            if r.status_code != 200:
                self.stderr.write(f"American Hospital API error {r.status_code}")
                break

            items = r.json().get("items", [])
            if not items:
                break

            for job in items[0].get("requisitionList", []):
                if count >= max_jobs:
                    break

                job_id = job.get("Id")
                job_url = f"https://fa-epvs-saasfaprod1.fa.ocs.oraclecloud.com/hcmUI/CandidateExperience/en/sites/CX_1/requisitions/preview/{job_id}"

                posted_date = None
                if job.get("PostedDate"):
                    try:
                        posted_date = datetime.strptime(job["PostedDate"], "%Y-%m-%d").date()
                    except:
                        pass

                Job.objects.update_or_create(
                    external_id=f"american-{job_id}",
                    defaults={
                        "title": job.get("Title", ""),
                        "location": job.get("PrimaryLocation", ""),
                        "hospital_name": "American Hospital",
                        "posted_date": posted_date,
                        "job_url": job_url,
                    }
                )
                count += 1

        self.stdout.write(self.style.SUCCESS(f"Imported/Updated {count} American Hospital jobs"))
        return count

    def handle(self, *args, **options):
        total = 0
        total += self.fetch_seha()
        total += self.fetch_nmc()
        total += self.fetch_burjeel()
        total += self.fetch_skmc()
        total += self.fetch_aster()
        total += self.fetch_american_hospital()

        self.stdout.write(self.style.SUCCESS(f"Imported/Updated {total} jobs (SEHA + NMC + Burjeel + SKMC)"))
