import csv
import time
import os
from dotenv import load_dotenv
import smtplib
from email.message import EmailMessage
from jobspy import scrape_jobs
import pandas as pd


OUTPUT_FILE = "jobs.csv"

# Load environment variables from .env
load_dotenv()


def send_email_alert(new_jobs_df):
    """
    Optional: Email alert when new jobs are found.
    Fill in your email + app password if you want this enabled.
    """

    SENDER = os.getenv("SENDER")
    PASSWORD = os.getenv("APP_PASSWORD")
    RECEIVER = os.getenv("RECEIVER")

    msg = EmailMessage()
    msg["Subject"] = f"{len(new_jobs_df)} New Retail Jobs Found"
    msg["From"] = SENDER
    msg["To"] = RECEIVER

    body = "New Jobs Found:\n\n"

    for _, row in new_jobs_df.iterrows():
        body += f"{row['title']} - {row['company']}\n"
        body += f"{row['location']}\n"
        body += f"{row['job_url']}\n\n"

    msg.set_content(body)

    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as smtp:
        smtp.login(SENDER, PASSWORD)
        smtp.send_message(msg)


def run_job_scraper():
    print("Checking for new jobs...")

    jobs = scrape_jobs(
        site_name=["indeed", "linkedin", "zip_recruiter", "google"],
        search_term="Retail",
        google_search_term="retail jobs near Guildford, Surrey",
        location="Guildford, Surrey",
        distance=10,
        job_type="parttime",
        results_wanted=50,
        country_indeed="United Kingdom",
    )

    df = jobs

    important_columns = [
        "job_url",
        "job_url_direct",
        "title",
        "company",
        "location",
        "date_posted",
        "job_type",
        "min_amount",
        "max_amount",
        "currency",
        "is_remote"
    ]

    existing_columns = [col for col in important_columns if col in df.columns]
    df = df[existing_columns]

    df = df.dropna(subset=["job_url"])

    df = df[
        df["location"]
        .str.contains("Guildford", case=False, na=False)
    ]

    df = df.drop_duplicates(subset=["job_url"])

    # Load previous jobs
    if os.path.exists(OUTPUT_FILE):
        old_df = pd.read_csv(OUTPUT_FILE)
    else:
        old_df = pd.DataFrame(columns=df.columns)

    # Detect new jobs
    new_jobs = df[~df["job_url"].isin(old_df["job_url"])]

    # Merge old + new (never delete old)
    updated_df = pd.concat([old_df, new_jobs]).drop_duplicates(subset=["job_url"])

    if not new_jobs.empty:
        print(f"\n🚨 {len(new_jobs)} NEW JOBS FOUND!\n")

        for _, row in new_jobs.iterrows():
            print(f"{row['title']} - {row['company']}")
            print(row["job_url"])
            print("-" * 50)

        # Optional: enable email alert
        send_email_alert(new_jobs)

    else:
        print("No new jobs found.")

    # Save updated file
    updated_df.to_csv(OUTPUT_FILE, index=False)
    print(f"Updated {OUTPUT_FILE}")
    print("Done.\n")


# # Run every hour
# while True:
#     run_job_scraper()
#     print("Sleeping for 1 hour...\n")
#     time.sleep(3600)

run_job_scraper()