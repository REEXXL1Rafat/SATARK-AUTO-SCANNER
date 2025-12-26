import requests
import pandas as pd
import numpy as np  
import google.generativeai as genai
import time
import rasterio
import os
import json
from datetime import datetime

# ==========================================
# 🔐 SECURE CONFIGURATION (GITHUB MODE)
# ==========================================
# We use os.environ to read secrets from the System/GitHub Vault
# This prevents hackers from stealing your keys if you share the file.
try:
    NASA_KEY = os.environ.get("NASA_KEY")
    GEMINI_KEY = os.environ.get("GEMINI_KEY")
    TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
    TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")
    SUPABASE_URL = os.environ.get("SUPABASE_URL")
    SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
    
    # Validation Check
    if not all([NASA_KEY, GEMINI_KEY, TELEGRAM_BOT_TOKEN, SUPABASE_URL, SUPABASE_KEY]):
        print("⚠️ WARNING: Some secrets are missing from Environment Variables.")
        # Fallback for Local Testing (ONLY IF NEEDED - Ideally use .env locally)
        # You can temporarily hardcode here if testing on laptop again, 
        # but DELETE before uploading to GitHub.
except Exception as e:
    print(f"❌ CRITICAL ERROR: {e}")
    exit(1)

LAND_MAP_PATH = "land_map.tif"

# 🌍 GEO-FENCE: WEST BENGAL (Full State)
LAT_MIN, LAT_MAX = 21.5, 27.3
LON_MIN, LON_MAX = 85.8, 89.9

# ==========================================
# ☁️ CLOUD DATABASE LAYER (REST API)
# ==========================================
def save_fire_event(lat, lon, source, cluster_size):
    """
    Saves fire to Supabase using pure HTTP (No complex libraries).
    """
    # 1. Calculate Estimated Area
    base_pixel = 140000 
    if "MODIS" in source: base_pixel = 1000000
    if "HIMAWARI" in source: base_pixel = 4000000
    est_area = (base_pixel * cluster_size) * 0.15 
    
    # 2. ID Generation
    lat_r, lon_r = round(lat, 3), round(lon, 3)
    fire_id = f"{lat_r}_{lon_r}"

    # 3. Prepare Data
    current_time = datetime.now().isoformat()
    
    # Headers for Supabase REST API
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "return=minimal" 
    }

    try:
        # STEP A: Check if fire exists (GET Request)
        check_url = f"{SUPABASE_URL}/rest/v1/fires?id=eq.{fire_id}&select=alert_count"
        response = requests.get(check_url, headers=headers)
        existing_data = response.json()

        if existing_data:
            # STEP B: UPDATE (Patch)
            new_count = existing_data[0]['alert_count'] + 1
            update_payload = {
                "last_seen": current_time,
                "alert_count": new_count,
                "est_area_m2": est_area
            }
            requests.patch(f"{SUPABASE_URL}/rest/v1/fires?id=eq.{fire_id}", headers=headers, json=update_payload)
            return False, new_count, est_area
        else:
            # STEP C: INSERT (Post)
            new_payload = {
                "id": fire_id,
                "lat": lat,
                "lon": lon,
                "first_seen": current_time,
                "last_seen": current_time,
                "alert_count": 1,
                "source": source,
                "est_area_m2": est_area
            }
            requests.post(f"{SUPABASE_URL}/rest/v1/fires", headers=headers, json=new_payload)
            return True, 1, est_area

    except Exception as e:
        print(f"       ⚠️ CLOUD ERROR: {e}")
        return False, 0, est_area

# ==========================================
# 🌍 TRUTH LAYER
# ==========================================
def check_land_type(lat, lon):
    if not os.path.exists(LAND_MAP_PATH): return "UNKNOWN"
    try:
        with rasterio.open(LAND_MAP_PATH) as src:
            if not (src.bounds.left < lon < src.bounds.right and src.bounds.bottom < lat < src.bounds.top):
                return "OUT_OF_BOUNDS"
            row, col = src.index(lon, lat)
            window = rasterio.windows.Window(col, row, 1, 1)
            data = src.read(1, window=window)
            val = data[0][0]
            if val == 40: return "FARM"
            if val == 50: return "URBAN"
            if val == 10: return "FOREST"
            return "OTHER"
    except: return "UNKNOWN"

# ==========================================
# 🧠 AI ANALYST
# ==========================================
def analyze_fire_event(lat, lon, count, land_type, area):
    try:
        genai.configure(api_key=GEMINI_KEY)
        model = genai.GenerativeModel('gemini-2.0-flash') 
        prompt = f"""
        ACT AS: SATARK, West Bengal Fire Monitor.
        DATA: {lat}, {lon} | Type: {land_type} | Est. Burn Size: {area} sq. meters.
        TASK: If Type is URBAN, Decision NO. If FARM/FOREST, Decision YES.
        OUTPUT: DECISION: [YES/NO] \n MESSAGE: [Short Alert for BDO]
        """
        response = model.generate_content(prompt)
        return response.text.strip()
    except:
        return f"DECISION: YES\nMESSAGE: 🚨 MANUAL CHECK: {lat}, {lon}"

def send_telegram(message):
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        requests.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "text": message})
        print("       🚀 Alert Sent.")
    except: pass

# ==========================================
# 🛰️ MAIN ENGINE (GITHUB MODE: SINGLE RUN)
# ==========================================
def scan_sector():
    print(f"\n▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬")
    print(f"☁️  SATARK V5.0 (GITHUB SENTINEL) | WB SCAN | {datetime.now()}")
    print(f"▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬")
    
    satellites = {
        "MODIS_TERRA": f"https://firms.modaps.eosdis.nasa.gov/api/area/csv/{NASA_KEY}/MODIS_NRT/world/1",
        "VIIRS_SNPP": f"https://firms.modaps.eosdis.nasa.gov/api/area/csv/{NASA_KEY}/VIIRS_SNPP_NRT/world/1",
        "VIIRS_NOAA20": f"https://firms.modaps.eosdis.nasa.gov/api/area/csv/{NASA_KEY}/VIIRS_NOAA20_NRT/world/1",
        "HIMAWARI": f"https://firms.modaps.eosdis.nasa.gov/api/country/csv/{NASA_KEY}/HIMAWARI/IND/1"
    }
    
    found_fires = []

    for sat_name, url in satellites.items():
        try:
            print(f"   🛰️  UPLINKING {sat_name:<15} ...", end=" ")
            try: df = pd.read_csv(url)
            except: 
                print("❌ [LINK DOWN]")
                continue

            df.columns = [c.lower() for c in df.columns]
            if 'latitude' not in df.columns: 
                if 'lat' in df.columns: df.rename(columns={'lat': 'latitude', 'lon': 'longitude'}, inplace=True)
                else: continue

            # Filter for WEST BENGAL Coords
            local_fires = df[
                (df['latitude'] >= LAT_MIN) & (df['latitude'] <= LAT_MAX) &
                (df['longitude'] >= LON_MIN) & (df['longitude'] <= LON_MAX)
            ]
            
            if not local_fires.empty:
                if 'confidence' in local_fires.columns:
                    local_fires = local_fires[local_fires['confidence'] != 'l']
                print(f"⚠️  [FOUND {len(local_fires)}]")
                local_fires['source'] = sat_name
                found_fires.append(local_fires)
            else:
                print("✅  [CLEAN]")
        except: continue

    if not found_fires:
        print("\n   ✅ WEST BENGAL CLEAR.")
        return

    # FUSION & LOGIC
    merged = pd.concat(found_fires)
    merged['lat_r'] = merged['latitude'].round(2) 
    merged['lon_r'] = merged['longitude'].round(2)
    
    unique_clusters = merged.groupby(['lat_r', 'lon_r']).agg({
        'latitude': 'mean', 'longitude': 'mean', 'source': 'first', 'brightness': 'count'
    }).reset_index()
    
    unique_clusters.rename(columns={'brightness': 'cluster_size'}, inplace=True)

    for _, fire in unique_clusters.iterrows():
        lat, lon = fire['latitude'], fire['longitude']
        source, size = fire['source'], fire['cluster_size']
        
        # SAVE TO CLOUD & GET BURN AREA
        is_new, alert_count, area_m2 = save_fire_event(lat, lon, source, size)

        print(f"\n   🔻 TARGET: {lat:.3f}, {lon:.3f} | SIZE: {area_m2:.0f} m² | {source}")

        if not is_new and alert_count > 3:
            print("       🔇 STATUS: MUTED (Recurring)")
            continue

        land_type = check_land_type(lat, lon)
        print(f"       🌍 LAND: {land_type}")
        
        if land_type == "URBAN":
            print("       🛑 IGNORED (Urban)")
            continue
            
        ai_msg = analyze_fire_event(lat, lon, size, land_type, int(area_m2))
        
        if "DECISION: YES" in ai_msg:
            raw = ai_msg.split("MESSAGE:")[1].strip() if "MESSAGE:" in ai_msg else ai_msg
            msg = (f"🔥 AGNI ALERT (WB)\n{raw}\n"
                   f"📏 Est. Burn: {area_m2:.0f} m²\n"
                   f"📍 {lat:.4f}, {lon:.4f}\n"
                   f"🔗 http://maps.google.com/?q={lat},{lon}")
            send_telegram(msg)
            time.sleep(1)

if __name__ == "__main__":
    scan_sector()
    # ⚠️ NOTE: We REMOVED the "while True" loop.

    # GitHub will run this script ONCE every 30 mins. It doesn't need to loop itself.
