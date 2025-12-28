import os
import pandas as pd
import requests
import smtplib
from email.mime.text import MIMEText
from datetime import datetime, timedelta

# ==========================================
# 🔐 CONFIGURATION
# ==========================================
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
SENDER_EMAIL = os.environ.get("SENDER_EMAIL")
GMAIL_PASSWORD = os.environ.get("GMAIL_APP_PASSWORD")
RECIPIENT_EMAIL = "reezaalarafat@gmail.com"

def send_email(subject, body):
    if not SENDER_EMAIL or not GMAIL_PASSWORD: 
        print("❌ Email Config Missing")
        return
        
    msg = MIMEText(body)
    msg['Subject'] = subject
    msg['From'] = SENDER_EMAIL
    msg['To'] = RECIPIENT_EMAIL
    
    try:
        with smtplib.SMTP("smtp.gmail.com", 587) as server:
            server.starttls()
            server.login(SENDER_EMAIL, GMAIL_PASSWORD)
            server.send_message(msg)
        print("✅ Weekly Dispatch Sent.")
    except Exception as e:
        print(f"❌ Email Failed: {e}")

def run_weekly_check():
    print(f"🕵️ WEEKLY INTEL | {datetime.now().strftime('%Y-%m-%d')}")
    
    # 1. FETCH LAST 7 DAYS FROM DB
    # We filter by 'last_seen' to get active fires from this week
    last_week = (datetime.now() - timedelta(days=7)).isoformat()
    
    headers = {
        "apikey": SUPABASE_KEY, 
        "Authorization": f"Bearer {SUPABASE_KEY}"
    }
    
    # We select 'frp_mw' to report Intensity
    url = f"{SUPABASE_URL}/rest/v1/fires?select=*&last_seen=gte.{last_week}"
    
    try:
        r = requests.get(url, headers=headers)
        data = r.json()
    except Exception as e:
        print(f"❌ DB Connection Failed: {e}")
        return

    if not data:
        print("✅ No fires detected this week. System Clean.")
        return

    df = pd.DataFrame(data)
    
    # 2. CALCULATE METRICS
    # Handle cases where column might be missing (rare but safe)
    if 'frp_mw' not in df.columns: df['frp_mw'] = 0.0
    
    total_fires = len(df)
    max_intensity = df['frp_mw'].max()
    avg_intensity = df['frp_mw'].mean()
    total_energy = df['frp_mw'].sum()
    
    # breakdown by Zone
    if 'location' in df.columns:
        zone_counts = df['location'].value_counts().to_string()
    else:
        zone_counts = "No Zone Data"
    
    # 3. GENERATE STATUS REPORT
    body = f"""
    SATARK WEEKLY SITREP
    --------------------
    📅 Period: Last 7 Days
    🔥 Total Incidents: {total_fires}
    
    ⚡ INTENSITY METRICS:
    - Max Peak: {max_intensity:.2f} MW
    - Average: {avg_intensity:.2f} MW
    - Total Energy: {total_energy:.2f} MW
    
    📍 ACTIVITY ZONES:
    {zone_counts}
    
    --------------------
    SYSTEM STATUS:
    - Database: Connected (Supabase)
    - Scanner: Active
    - Next Audit: 1st of Next Month
    """
    
    send_email(f"SATARK Weekly: {total_fires} Fires Detected", body)

if __name__ == "__main__":
    run_weekly_check()
