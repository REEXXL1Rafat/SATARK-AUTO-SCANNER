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
# 🔐 CONFIGURATION: HYBRID MODE
# ==========================================
try:
    NASA_KEY = os.environ.get("NASA_KEY")
    GEMINI_KEY = os.environ.get("GEMINI_KEY")
    TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
    TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")
    SUPABASE_URL = os.environ.get("SUPABASE_URL")
    SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
    
    if not all([NASA_KEY, GEMINI_KEY, TELEGRAM_BOT_TOKEN, SUPABASE_URL, SUPABASE_KEY]):
        print("⚠️ WARNING: Missing Keys. Check GitHub Secrets.")
except Exception as e:
    print(f"❌ CRITICAL ERROR: {e}")
    exit(1)

LAND_MAP_PATH = "land_map.tif"

# 🌍 1. PAN-INDIA BOUNDS (For Data Harvest)
INDIA_LAT_MIN, INDIA_LAT_MAX = 6.0, 37.0
INDIA_LON_MIN, INDIA_LON_MAX = 68.0, 97.0
INDIA_BOUNDS_STR = f"{INDIA_LON_MIN},{INDIA_LAT_MIN},{INDIA_LON_MAX},{INDIA_LAT_MAX}"

# 🎯 2. KILL BOX: WEST BENGAL (For Alerts)
WB_LAT_MIN, WB_LAT_MAX = 21.5, 27.3
WB_LON_MIN, WB_LON_MAX = 85.8, 89.9

# ==========================================
# 🗺️ ZONING LOGIC (Research Tags)
# ==========================================
def get_research_zone(lat, lon):
    """Tags the fire for your Research Paper"""
    if lat > 28.0: return "ZONE_A_NORTH"      # Punjab/Haryana
    if (20.0 <= lat <= 28.0) and (lon > 84.0): return "ZONE_B_EAST" # WB/Bihar
    if lat < 20.0: return "ZONE_C_SOUTH"      # South India
    return "ZONE_D_CENTRAL"

# ==========================================
# ☁️ CLOUD DATABASE LAYER (Supabase)
# ==========================================
def save_fire_event(lat, lon, source, cluster_size, zone):
    """
    Saves fire to Supabase with the new 'location' tag.
    """
    # Area Estimation Logic
    if "MODIS" in source: base_pixel = 1000000     # 1km pixel
    elif "HIMAWARI" in source: base_pixel = 4000000 # 2km pixel
    else: base_pixel = 140000                      # VIIRS (375m pixel)
    
    est_area = (base_pixel * cluster_size) * 0.15 
    
    lat_r, lon_r = round(lat, 3), round(lon, 3)
    fire_id = f"{lat_r}_{lon_r}"
    current_time = datetime.now().isoformat()
    
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "return=minimal" 
    }

    try:
        # Check if fire exists
        check_url = f"{SUPABASE_URL}/rest/v1/fires?id=eq.{fire_id}&select=alert_count"
        response = requests.get(check_url, headers=headers)
        existing_data = response.json()

        if existing_data:
            new_count = existing_data[0]['alert_count'] + 1
            update_payload = {
                "last_seen": current_time,
                "alert_count": new_count,
                "est_area_m2": est_area
            }
            requests.patch(f"{SUPABASE_URL}/rest/v1/fires?id=eq.{fire_id}", headers=headers, json=update_payload)
            return False, new_count, est_area
        else:
            new_payload = {
                "id": fire_id,
                "lat": lat,
                "lon": lon,
                "first_seen": current_time,
                "last_seen": current_time,
                "alert_count": 1,
                "source": source,
                "est_area_m2": est_area,
                "location": zone  # <--- The New Column!
            }
            requests.post(f"{SUPABASE_URL}/rest/v1/fires", headers=headers, json=new_payload)
            return True, 1, est_area

    except Exception as e:
        print(f"       ⚠️ CLOUD ERROR: {e}")
        return False, 0, est_area

# ==========================================
# 🌍 TRUTH LAYER (Map Check - WB Only)
# ==========================================
def check_land_type(lat, lon):
    if not (WB_LAT_MIN <= lat <= WB_LAT_MAX and WB_LON_MIN <= lon <= WB_LON_MAX):
        return "UNKNOWN (OUTSIDE WB)"

    if not os.path.exists(LAND_MAP_PATH): return "UNKNOWN (NO MAP)"
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
# 🧠 AI ANALYST (WB Only)
# ==========================================
def analyze_fire_event(lat, lon, land_type, area):
    try:
        genai.configure(api_key=GEMINI_KEY)
        model = genai.GenerativeModel('gemini-2.0-flash') 
        prompt = f"""
        ACT AS: Disaster Management Officer.
        DATA: {lat}, {lon} | Type: {land_type} | Burn Size: {area} m2.
        TASK: Draft a 1-sentence urgent alert.
        """
        response = model.generate_content(prompt)
        return response.text.strip()
    except:
        return f"DETECTED {land_type} FIRE ({area} m2)"

def send_telegram(message):
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        requests.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "text": message})
        print("       🚀 Alert Sent.")
    except: pass

# ==========================================
# 🛰️ MAIN ENGINE (5-SAT ARRAY)
# ==========================================
def scan_sector():
    print(f"\n▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬")
    print(f"☁️  SATARK V7.1 (5-SAT ARRAY) | INDIA SCAN | {datetime.now()}")
    print(f"▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬")
    
    # THE FULL ARSENAL
    satellites = {
        "VIIRS_NOAA20": f"https://firms.modaps.eosdis.nasa.gov/api/area/csv/{NASA_KEY}/VIIRS_NOAA20_NRT/{INDIA_BOUNDS_STR}/1",
        "VIIRS_SNPP":   f"https://firms.modaps.eosdis.nasa.gov/api/area/csv/{NASA_KEY}/VIIRS_SNPP_NRT/{INDIA_BOUNDS_STR}/1",
        "MODIS_NRT":    f"https://firms.modaps.eosdis.nasa.gov/api/area/csv/{NASA_KEY}/MODIS_NRT/{INDIA_BOUNDS_STR}/1",
        "HIMAWARI":     f"https://firms.modaps.eosdis.nasa.gov/api/country/csv/{NASA_KEY}/HIMAWARI/IND/1"
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
            # Normalize Columns (Himawari uses different names sometimes)
            if 'latitude' not in df.columns: 
                if 'lat' in df.columns: df.rename(columns={'lat': 'latitude', 'lon': 'longitude'}, inplace=True)
                else: continue

            if not df.empty:
                print(f"⚠️  [FOUND {len(df)}]")
                df['source'] = sat_name
                found_fires.append(df)
            else:
                print("✅  [CLEAN]")
        except: continue

    if not found_fires:
        print("\n   ✅ INDIA SECTOR CLEAR (ALL SATELLITES).")
        return

    # FUSION
    merged = pd.concat(found_fires)
    merged['lat_r'] = merged['latitude'].round(2) 
    merged['lon_r'] = merged['longitude'].round(2)
    
    unique_clusters = merged.groupby(['lat_r', 'lon_r']).agg({
        'latitude': 'mean', 'longitude': 'mean', 'source': 'first', 'brightness': 'count'
    }).reset_index()
    unique_clusters.rename(columns={'brightness': 'cluster_size'}, inplace=True)

    print(f"\n   🔻 PROCESSING {len(unique_clusters)} CLUSTERS...")

    for _, fire in unique_clusters.iterrows():
        lat, lon = fire['latitude'], fire['longitude']
        source, size = fire['source'], fire['cluster_size']
        
        # 1. TAG ZONE
        zone = get_research_zone(lat, lon)
        
        # 2. SAVE TO CLOUD (Research Data - All India)
        is_new, alert_count, area_m2 = save_fire_event(lat, lon, source, size, zone)

        # 3. ALERT LOGIC (Filter for West Bengal ONLY)
        is_critical_wb = (zone == "ZONE_B_EAST") and (WB_LAT_MIN <= lat <= WB_LAT_MAX)

        if is_critical_wb:
            print(f"   🔥 CRITICAL WB TARGET: {lat:.3f}, {lon:.3f}")
            
            # Check Land Map (Only for WB)
            land_type = check_land_type(lat, lon)
            
            if land_type == "URBAN":
                print("       🛑 IGNORED (Urban)")
                continue

            if not is_new and alert_count > 3:
                print("       🔇 STATUS: MUTED (Recurring)")
                continue
                
            # Generate AI Alert
            ai_msg = analyze_fire_event(lat, lon, land_type, int(area_m2))
            
            msg = (f"🔥 AGNI ALERT (WB)\n{ai_msg}\n"
                   f"📏 Est. Burn: {area_m2:.0f} m²\n"
                   f"📍 {lat:.4f}, {lon:.4f}\n"
                   f"🔗 http://maps.google.com/?q={lat},{lon}")
            send_telegram(msg)
            time.sleep(1)
            
        else:
            print(f"   📝 LOGGED: {zone} (No Alert)")

if __name__ == "__main__":
    scan_sector()
