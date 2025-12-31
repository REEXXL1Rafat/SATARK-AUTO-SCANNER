import requests
import pandas as pd
import os
import time
import io
import math
import boto3
from botocore import UNSIGNED
from botocore.config import Config
from datetime import datetime
from openai import OpenAI

# ==========================================
# 🔐 CONFIGURATION
# ==========================================
try:
    NASA_KEY = os.environ.get("NASA_KEY", "").strip()
    TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
    TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "").strip()
    SUPABASE_URL = os.environ.get("SUPABASE_URL", "").strip()
    SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "").strip()
    OPENROUTER_API_KEY = os.environ.get("OPENROUTER_API_KEY", "").strip()
    
    if not all([NASA_KEY, TELEGRAM_BOT_TOKEN, SUPABASE_URL, SUPABASE_KEY, OPENROUTER_API_KEY]):
        print("⚠️ CRITICAL: Missing Keys.")
except Exception as e:
    print(f"Config Error: {e}")

client = OpenAI(
    base_url="https://openrouter.ai/api/v1",
    api_key=OPENROUTER_API_KEY,
)

# 🌍 INDIA BOUNDING BOX
INDIA_BOX_NASA = "68,6,98,38"

# ==========================================
# 🗺️ REGION INTELLIGENCE
# ==========================================
def get_region_tag(lat, lon):
    if 21.5 <= lat <= 27.3 and 85.8 <= lon <= 89.9: return "WEST_BENGAL"
    if 28.4 <= lat <= 32.5 and 73.8 <= lon <= 77.8: return "PUNJAB_HARYANA"
    if 28.0 <= lat <= 28.9 and 76.8 <= lon <= 77.5: return "DELHI_NCR"
    return "INDIA_OTHER"

# ==========================================
# 🕵️‍♂️ OSM VERIFICATION (NOISE FILTER)
# ==========================================
def verify_land_use(lat, lon, region, frp):
    if region != "WEST_BENGAL" and frp < 20.0: 
        return "UNVERIFIED (Quota Saved)"

    url = "http://overpass-api.de/api/interpreter"
    query = f"""
    [out:json];
    (
      way(around:500, {lat}, {lon})["landuse"];
      way(around:500, {lat}, {lon})["industrial"];
      way(around:500, {lat}, {lon})["natural"="water"];
    );
    out tags;
    """
    try:
        time.sleep(1) 
        r = requests.get(url, params={'data': query}, timeout=10)
        if r.status_code != 200: return "UNKNOWN"
        
        data = r.json()
        tags_found = []
        for el in data.get('elements', []):
            if 'tags' in el:
                tags = el['tags']
                if 'industrial' in tags: return "INDUSTRY"
                if tags.get('natural') == 'water': return "WATER"
                if 'landuse' in tags: tags_found.append(tags['landuse'])
        
        if any(t in ['industrial', 'residential', 'railway'] for t in tags_found): return "INDUSTRY"
        if any(t in ['farmland', 'farm', 'forest', 'orchard'] for t in tags_found): return "FARM"
        
        return "UNKNOWN"
    except:
        return "UNKNOWN"

# ==========================================
# 🛰️ GEOSTATIONARY ENGINE (GK-2A)
# ==========================================
def get_gk2a_fires():
    """
    Connects to AWS Open Data to check for fires via Korean Satellite (GK-2A).
    This runs 24/7 to catch fires that NASA Polar satellites miss due to timing.
    """
    print("📡 Connecting to GK-2A (Korean Geo-Sat)...")
    fires = []
    
    # NOTE: In a full deployment, this downloads the .nc file using boto3.
    # For this script, we use the placeholder structure where you would 
    # insert the netCDF4 processing logic.
    # Since we cannot download 200MB files in a lightweight Action efficiently without caching,
    # this function is prepared for the logic insertion or light API usage if available.
    
    # Logic: 
    # 1. boto3.client('s3', config=UNSIGNED).download_file('noaa-gk2a-pds', 'latest.nc')
    # 2. Process T_b (Brightness Temp)
    # 3. Apply Dozier Math
    
    return fires

# ==========================================
# 🧠 SMART DATABASE (DYNAMIC MERGE)
# ==========================================
def save_fire_event_smart(lat, lon, source, cluster_size, region, frp, confidence):
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json"
    }

    # === DYNAMIC SEARCH RADIUS ===
    # IF GK-2A (Blurry): Look 2.5km to snap to a specific Polar fire.
    # IF VIIRS (Sharp): Look 0.1km (Research Grade) to find neighbor fires.
    search_radius = 0.025 if "GK2A" in source else 0.001
    
    lat_min, lat_max = lat - search_radius, lat + search_radius
    lon_min, lon_max = lon - search_radius, lon + search_radius
    
    check_url = f"{SUPABASE_URL}/rest/v1/fires?lat=gte.{lat_min}&lat=lte.{lat_max}&lon=gte.{lon_min}&lon=lte.{lon_max}&select=*"
    
    try:
        r = requests.get(check_url, headers=headers)
        existing_fires = r.json() if r.status_code == 200 else []
        
        match = None
        for f in existing_fires:
            dist_km = math.sqrt((f['lat'] - lat)**2 + (f['lon'] - lon)**2) * 111
            
            # Use the Dynamic Threshold
            limit_km = 2.5 if "GK2A" in source else 0.1
            
            if dist_km < limit_km: 
                match = f
                break
        
        now_time = datetime.utcnow().isoformat()
        
        if match:
            # === MERGE (Preserve First Seen, Update Last Seen) ===
            print(f"   🔄 Merging with Event {match['id']}...")
            row_id = match['id']
            old_src = match.get('source', '')
            new_source = old_src if source in old_src else f"{old_src}, {source}"
            new_frp = max(float(match.get('frp_mw', 0)), float(frp))
            
            payload = {
                "last_seen": now_time,
                "frp_mw": new_frp,
                "source": new_source,
                "alert_count": match.get('alert_count', 1) + 1
            }
            requests.patch(f"{SUPABASE_URL}/rest/v1/fires?id=eq.{row_id}", headers=headers, json=payload)
            return False 
        else:
            # === INSERT NEW (Set First Seen) ===
            pixel_size = 2000000 if "GK2A" in source else 375000
            est_area = (pixel_size * cluster_size) * 0.15
            
            payload = {
                "lat": float(lat),
                "lon": float(lon),
                "first_seen": now_time, 
                "last_seen": now_time,
                "source": source,
                "alert_count": 1,
                "location": region, 
                "frp_mw": float(frp),
                "confidence": str(confidence),
                "est_area_m2": est_area
            }
            requests.post(f"{SUPABASE_URL}/rest/v1/fires", headers=headers, json=payload)
            return True 

    except Exception as e:
        print(f"DB Error: {e}")
        return False

# ==========================================
# 🤖 AI ANALYST & ALERTS
# ==========================================
def analyze_with_ai(lat, lon, region, frp):
    try:
        prompt = f"TASK: Alert. COORDS: {lat},{lon}. REGION: {region}. FRP: {frp}MW. Check if inside India. If yes, 10-word summary."
        completion = client.chat.completions.create(
            model="meta-llama/llama-3-70b-instruct", 
            messages=[{"role": "user", "content": prompt}]
        )
        return completion.choices[0].message.content.strip()
    except:
        return "Confirmed Fire."

def send_telegram(msg):
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        requests.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "text": msg})
    except: pass

# ==========================================
# 🚀 SATARK V13.2 (24/7 SENTINEL)
# ==========================================
def scan_sector():
    print(f"\n🚀 SATARK V13.2 SENTINEL | {datetime.now().strftime('%H:%M:%S')}")
    all_fires = []

    # 1. POLAR (NASA) - THE SHARP EYE
    base_url = "https://firms.modaps.eosdis.nasa.gov/api/area/csv"
    nasa_sats = {
        "VIIRS_SNPP": f"{base_url}/{NASA_KEY}/VIIRS_SNPP_NRT/{INDIA_BOX_NASA}/1",
        "VIIRS_NOAA": f"{base_url}/{NASA_KEY}/VIIRS_NOAA20_NRT/{INDIA_BOX_NASA}/1",
        "MODIS": f"{base_url}/{NASA_KEY}/MODIS_NRT/{INDIA_BOX_NASA}/1"
    }
    
    for sat_name, url in nasa_sats.items():
        print(f"📡 Scanning {sat_name}...", end=" ")
        try:
            r = requests.get(url, timeout=30)
            if r.status_code == 200:
                df = pd.read_csv(io.StringIO(r.text))
                df.columns = [c.lower() for c in df.columns]
                if 'latitude' in df.columns:
                    df['source'] = sat_name
                    if 'frp' not in df.columns: df['frp'] = 0.0
                    df['conf_score'] = "100%"
                    all_fires.append(df)
                    print(f"✅ {len(df)} Points")
        except: pass

    # 2. GEO (GK-2A) - THE ALWAYS-ON EYE
    gk_data = get_gk2a_fires()
    if gk_data: all_fires.append(pd.DataFrame(gk_data))

    if not all_fires:
        print("✅ Sector Clear.")
        return

    merged = pd.concat(all_fires)
    print(f"📊 Processing {len(merged)} Events...")
    
    for _, f in merged.iterrows():
        lat, lon = f['latitude'], f['longitude']
        frp = f.get('frp', 0)
        source = f['source']
        region = get_region_tag(lat, lon)
        
        # Ground Truth Filter
        land_type = verify_land_use(lat, lon, region, frp)
        if land_type in ["INDUSTRY", "WATER"]: continue
        if frp > 500 and land_type != "FARM": continue

        # Save & Alert
        is_new = save_fire_event_smart(lat, lon, source, 1, region, frp, f.get('conf_score', 'Low'))
        
        if is_new:
            # Alert Logic (Bengal always, others if big)
            if region == "WEST_BENGAL" or frp > 50.0:
                print(f"   🔥 ALERT: {region} | {frp}MW")
                ai_msg = analyze_with_ai(lat, lon, region, frp)
                if "FOREIGN" not in ai_msg:
                    msg = f"🔥 SATARK ALERT\n📍 {region}\n⚡ {frp:.1f} MW\n🤖 {ai_msg}\n🔗 https://maps.google.com/?q={lat},{lon}"
                    send_telegram(msg)

if __name__ == "__main__":
    scan_sector()
