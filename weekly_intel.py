import os
import pandas as pd
import requests
from datetime import datetime, timedelta

# 1. SETUP CREDENTIALS
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
# CHECK THIS: Is your table named 'fires' or 'fire_alerts'? 
# I have set it to 'fire_alerts' as per standard. Change if needed.
TABLE_NAME = "fires" 

def fetch_and_clean():
    print(f"🕵️ WEEKLY INTEL BOT | {datetime.now()}")
    
    # 2. CALCULATE DATE RANGE (Last 7 Days)
    today = datetime.now()
    last_week = today - timedelta(days=7)
    
    # 3. FETCH DATA FROM SUPABASE
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}"
    }

    # We select rows created in the last 7 days
    # NOTE: If your database uses 'acq_date' or 'first_seen', change 'created_at' below.
    url = f"{SUPABASE_URL}/rest/v1/{TABLE_NAME}?select=*&created_at=gte.{last_week.isoformat()}"
    
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        data = response.json()
    except Exception as e:
        print(f"❌ Connection Failed: {e}")
        return

    if not data:
        print("✅ No fires detected this week. Skipping report.")
        return

    # 4. CREATE DATAFRAME (Fixed logic)
    df = pd.DataFrame(data)
    print(f"📥 Downloaded {len(df)} raw logs.")

    # 5. STANDARDIZE COLUMNS
    # Safety: Supabase might send 'latitude', but we need 'lat'
    if 'latitude' in df.columns:
        df.rename(columns={'latitude': 'lat', 'longitude': 'lon'}, inplace=True)

    # 6. APPLY "IRON DOME" CLEANING
    if 'lon' in df.columns:
        # Filter Myanmar/Andaman (Keep only West of 92.5E)
        clean_df = df[df['lon'] < 92.5].copy()
    else:
        print("⚠️ Warning: 'lon' column missing. Skipping geographic filter.")
        clean_df = df.copy()

    # 7. APPLY ZONING
    def assign_zone(row):
        lat, lon = row.get('lat', 0), row.get('lon', 0)
        if 28.0 <= lat <= 32.5 and 73.0 <= lon <= 77.5: return "ZONE_A_NORTH"
        if 21.5 <= lat <= 27.0 and 85.5 <= lon <= 89.9: return "ZONE_B_EAST"
        if 18.0 <= lat <= 24.0 and 80.0 <= lon <= 85.0: return "ZONE_D_CENTRAL"
        if 10.0 <= lat <= 20.0 and 73.0 <= lon <= 80.0: return "ZONE_C_SOUTH"
        return "ZONE_OTHER"

    clean_df['research_zone'] = clean_df.apply(assign_zone, axis=1)

    # 8. SAVE REPORT
    if not os.path.exists('weekly_reports'):
        os.makedirs('weekly_reports')
        
    filename = f"weekly_reports/intel_{today.strftime('%Y-%m-%d')}.csv"
    clean_df.to_csv(filename, index=False)
    
    print(f"💎 Report Generated: {filename} | Rows: {len(clean_df)}")

if __name__ == "__main__":
    fetch_and_clean()
