import os
import pandas as pd
import requests
from datetime import datetime, timedelta

# 1. SETUP CREDENTIALS (From GitHub Secrets)
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
TABLE_NAME = "fires" # Or 'fire_logs', make sure this matches DB

def fetch_and_clean():
    print(f"🕵️ WEEKLY INTEL BOT | {datetime.now()}")
    
    # 2. CALCULATE DATE RANGE (Last 7 Days)
    today = datetime.now()
    last_week = today - timedelta(days=7)
    
    # 3. FETCH DATA FROM SUPABASE (REST API)
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}"
    }
    # Query: Select ALL rows where 'first_seen' is newer than 7 days ago
    # We use 'gte' (Greater Than or Equal) logic
    url = f"{SUPABASE_URL}/rest/v1/{TABLE_NAME}?select=*&first_seen=gte.{last_week.isoformat()}"
    
    try:
        response = requests.get(url, headers=headers)
        data = response.json()
    except Exception as e:
        print(f"❌ Connection Failed: {e}")
        return

    if not data:
        print("✅ No fires detected this week. Skipping report.")
        return

    df = pd.read_json(pd.io.json.dumps(data))
    print(f"📥 Downloaded {len(df)} raw logs.")

    # 4. APPLY "IRON DOME" CLEANING
    # Filter Imposters
    if 'lon' in df.columns:
        clean_df = df[df['lon'] < 92.5].copy()
    else:
        clean_df = df.copy() # Fallback if columns missing

    # Apply Zoning
    def assign_zone(row):
        lat, lon = row.get('lat', 0), row.get('lon', 0)
        if 28.0 <= lat <= 32.5 and 73.0 <= lon <= 77.5: return "ZONE_A_NORTH"
        if 21.5 <= lat <= 27.0 and 85.5 <= lon <= 89.9: return "ZONE_B_EAST"
        if 18.0 <= lat <= 24.0 and 80.0 <= lon <= 85.0: return "ZONE_D_CENTRAL"
        if 10.0 <= lat <= 20.0 and 73.0 <= lon <= 80.0: return "ZONE_C_SOUTH"
        return "ZONE_OTHER"

    clean_df['research_zone'] = clean_df.apply(assign_zone, axis=1)

    # 5. SAVE REPORT (With Date Timestamp)
    # We save it to a folder named 'weekly_reports'
    if not os.path.exists('weekly_reports'):
        os.makedirs('weekly_reports')
        
    filename = f"weekly_reports/intel_{today.strftime('%Y-%m-%d')}.csv"
    clean_df.to_csv(filename, index=False)
    
    print(f"💎 Report Generated: {filename} | Rows: {len(clean_df)}")

if __name__ == "__main__":
    fetch_and_clean()
