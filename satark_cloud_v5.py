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
# 🔐 CONFIGURATION
# ==========================================
try:
    NASA_KEY = os.environ.get("NASA_KEY")
    GEMINI_KEY = os.environ.get("GEMINI_KEY")
    TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
    TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")
    SUPABASE_URL = os.environ.get("SUPABASE_URL")
    SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
    
    if not all([NASA_KEY, GEMINI_KEY, TELEGRAM_BOT_TOKEN, SUPABASE_URL, SUPABASE_KEY]):
        # Fallback for local testing (Remove in production)
        pass 
except Exception as e:
    exit(1)

LAND_MAP_PATH = "land_map.tif"

# 🌍 GEO-FENCE: WEST BENGAL (Optimized for NASA Request)
# Format: min_lon, min_lat, max_lon, max_lat
WB_EXTENT = "85.8,21.5,89.9,27.3" 
LAT_MIN, LAT_MAX = 21.5, 27.3
LON_MIN, LON_MAX = 85.8, 89.9

# ==========================================
# 🧠 SMART DATABASE LAYER (RPC)
# ==========================================
def save_fire_event(lat, lon, source, cluster_size, region="ZONE_UNKNOWN"):
    """
    Calls the SQL function 'upsert_fire_cluster' to handle de-duplication logic.
    """
    # 1. Estimate Area
    base_pixel = 1000000 if "MODIS" in source else 375000 # VIIRS is finer
    if "HIMAWARI" in source: base_pixel = 4000000
    est_area = (base_pixel * cluster_size) * 0.15 

    # 2. Call Supabase RPC (Remote Procedure Call)
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json"
    }

    payload = {
        "lat_in": lat,
        "lon_in": lon,
        "source_in": source,
        "area_in": est_area,
        "region_in": region
    }

    try:
        # We call the SQL function we just created
        rpc_url = f"{SUPABASE_URL}/rest/v1/rpc/upsert_fire_cluster"
        response = requests.post(rpc_url, headers=headers, json=payload)
        
        if response.status_code == 200:
            result = response.json()
            # Returns: {'status': 'new'/'merged', 'id': '...'}
            is_new = (result.get('status') == 'new')
            return is_new, est_area
        else:
            print(f"⚠️ DB Error: {response.text}")
            return False, est_area

    except Exception as e:
        print(f"⚠️ Connection Error: {e}")
        return False, est_area

# ==========================================
# 🌍 TRUTH LAYER & AI (Kept Same)
# ==========================================
def check_land_type(lat, lon):
    if not os.path.exists(LAND_MAP_PATH): return "UNKNOWN"
    try:
        with rasterio.open(LAND_MAP_PATH) as src:
            if not (src.bounds.left < lon < src.bounds.right and src.bounds.bottom < lat < src.bounds.top):
                return "OUT_OF_BOUNDS"
            row, col = src.index(lon, lat)
            val = src.read(1, window=rasterio.windows.Window(col, row, 1, 1))[0][0]
            mapping = {40: "FARM", 50: "URBAN", 10: "FOREST"}
            return mapping.get(val, "OTHER")
    except: return "UNKNOWN"

def analyze_fire_event(lat, lon, land_type, area):
    try:
        genai.configure(api_key=GEMINI_KEY)
        model = genai.GenerativeModel('gemini-2.0-flash') 
        # Optimized Prompt: Minimal tokens, strict output
        prompt = f"""
        Role: Fire Marshal.
        Input: {lat},{lon} | {land_type} | {area}m².
        Rule: If URBAN or <500m², IGNORE. Else ALERT.
        Output: DECISION: [YES/NO] | MSG: [10-word summary]
        """
        response = model.generate_content(prompt)
        return response.text.strip()
    except:
        return "DECISION: YES | MSG: Manual Verification Required."

def send_telegram(message):
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        requests.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "text": message})
    except: pass

# ==========================================
# 🛰️ MAIN ENGINE (OPTIMIZED)
# ==========================================
def scan_sector():
    print(f"\n🚀 SATARK V6.0 (OPTIMIZED) | {datetime.now().strftime('%H:%M:%S')}")
    
    # 1. DYNAMIC URLS (Bounding Box for Efficiency)
    # Using 'area/csv' instead of 'world' saves 99% bandwidth
    base_url = "https://firms.modaps.eosdis.nasa.gov/api/area/csv"
    
    satellites = {
        "MODIS": f"{base_url}/{NASA_KEY}/MODIS_NRT/{WB_EXTENT}/1",
        "VIIRS_SNPP": f"{base_url}/{NASA_KEY}/VIIRS_SNPP_NRT/{WB_EXTENT}/1",
        "VIIRS_NOAA": f"{base_url}/{NASA_KEY}/VIIRS_NOAA20_NRT/{WB_EXTENT}/1",
        # Himawari doesn't support bbox easily, stick to country
        "HIMAWARI": f"https://firms.modaps.eosdis.nasa.gov/api/country/csv/{NASA_KEY}/HIMAWARI/IND/1"
    }
    
    found_fires = []

    for sat_name, url in satellites.items():
        try:
            print(f"📡 Pinging {sat_name}...", end=" ")
            try: 
                df = pd.read_csv(url)
                print("✅")
            except: 
                print("❌")
                continue

            df.columns = [c.lower() for c in df.columns]
            # Standardize Columns
            if 'latitude' not in df.columns and 'lat' in df.columns:
                df.rename(columns={'lat': 'latitude', 'lon': 'longitude'}, inplace=True)

            if df.empty or 'latitude' not in df.columns: continue

            # Double Check Filter (API isn't always perfect)
            local_fires = df[
                (df['latitude'] >= LAT_MIN) & (df['latitude'] <= LAT_MAX) &
                (df['longitude'] >= LON_MIN) & (df['longitude'] <= LON_MAX)
            ]
            
            if not local_fires.empty:
                # Filter low confidence
                if 'confidence' in local_fires.columns:
                    # Filter out 'low' confidence or < 50%
                    local_fires = local_fires[local_fires['confidence'].astype(str) != 'l']
                
                local_fires['source'] = sat_name
                found_fires.append(local_fires)
                print(f"   ⚠️ Detected {len(local_fires)} heat signatures.")

        except Exception as e: print(f"Error: {e}")

    if not found_fires:
        print("✅ Sector Clear.")
        return

    # FUSION
    merged = pd.concat(found_fires)
    # Grouping by 2 decimals (~1km) for initial python-side clustering
    merged['lat_r'] = merged['latitude'].round(2) 
    merged['lon_r'] = merged['longitude'].round(2)
    
    unique_clusters = merged.groupby(['lat_r', 'lon_r']).agg({
        'latitude': 'mean', 'longitude': 'mean', 'source': 'first', 'latitude': 'count'
    }).rename(columns={'latitude': 'cluster_size'}).reset_index()

    for _, fire in unique_clusters.iterrows():
        lat, lon = fire['lat_r'], fire['lon_r'] # Use rounded for cleaner logs, real for DB
        # Use the MEAN lat/lon from the cluster
        real_lat, real_lon = fire[2], fire[3] # Indexing might vary, careful.
        # Better:
        real_lat = fire['latitude'] if 'latitude' in fire else lat
        real_lon = fire['longitude'] if 'longitude' in fire else lon
        
        size = fire['cluster_size']
        source = fire['source']

        # Determine Region (Simple Logic)
        region = "ZONE_UNKNOWN"
        if 21.5 <= lat <= 23.0: region = "ZONE_C_SOUTH"
        elif 23.0 < lat <= 25.0: region = "ZONE_D_CENTRAL"
        elif lat > 26.0: region = "ZONE_A_NORTH"

        # 🧠 SMART SAVE
        is_new_fire, area_m2 = save_fire_event(real_lat, real_lon, source, size, region)

        if not is_new_fire:
            print(f"   🔄 Merged with existing fire at {lat}, {lon}")
            continue # Skip alerting for existing fires

        # IT IS NEW - ANALYZE
        land_type = check_land_type(lat, lon)
        print(f"   🔥 NEW THREAT: {lat}, {lon} | {land_type}")
        
        if land_type == "URBAN": continue
        
        ai_res = analyze_fire_event(lat, lon, land_type, int(area_m2))
        
        if "DECISION: YES" in ai_res or "YES" in ai_res:
             msg_body = ai_res.split("MSG:")[1].strip() if "MSG:" in ai_res else "Fire Detected"
             telegram_msg = (
                 f"🔥 SATARK ALERT (WB)\n"
                 f"📍 {lat:.4f}, {lon:.4f} ({region})\n"
                 f"🌍 {land_type} | 📏 {area_m2:.0f}m²\n"
                 f"🤖 AI: {msg_body}\n"
                 f"🔗 https://maps.google.com/?q={lat},{lon}"
             )
             send_telegram(telegram_msg)

if __name__ == "__main__":
    scan_sector()
