import requests
from requests.adapters import HTTPAdapter, Retry
import pandas as pd
import os
import time
import io
import math
import boto3
from botocore import UNSIGNED
from botocore.config import Config
from datetime import datetime, timedelta
from openai import OpenAI

# ==========================================
# 🔐 CONFIGURATION
# ==========================================
try:
    # 1. API KEYS
    NASA_KEY = os.environ.get("NASA_KEY", "").strip()
    TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
    SUPABASE_URL = os.environ.get("SUPABASE_URL", "").strip()
    SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "").strip()
    OPENROUTER_API_KEY = os.environ.get("OPENROUTER_API_KEY", "").strip()

    # 2. RECIPIENT LIST (From GitHub Secrets)
    ADMIN_ID = os.environ.get("TELEGRAM_CHAT_ID", "").strip()
    GUARDIAN_ID = os.environ.get("GUARDIAN_CHAT_ID", "").strip()
    
    # Filter out empty IDs to prevent errors
    RECIPIENT_LIST = [id_ for id_ in [ADMIN_ID, GUARDIAN_ID] if id_]

    if not all([NASA_KEY, TELEGRAM_BOT_TOKEN, SUPABASE_URL, SUPABASE_KEY, OPENROUTER_API_KEY]):
        print("⚠️ CRITICAL: Missing API Keys.")
        
    if not RECIPIENT_LIST:
        print("⚠️ CRITICAL: No Recipient IDs found in Environment Variables.")

except Exception as e:
    print(f"Config Error: {e}")

client = OpenAI(
    base_url="https://openrouter.ai/api/v1",
    api_key=OPENROUTER_API_KEY,
)

# 🌍 INDIA BOUNDING BOX
INDIA_BOX_NASA = "68,6,98,38"

# ==========================================
# 🔌 ROBUST DATABASE SESSION
# ==========================================
db_session = requests.Session()
retries = Retry(total=5, backoff_factor=0.5, status_forcelist=[500, 502, 503, 504])
db_session.mount('https://', HTTPAdapter(max_retries=retries))
db_session.headers.update({
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json"
})

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
        r = db_session.get(url, params={'data': query}, timeout=10)
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
# 🛰️ GEOSTATIONARY ENGINE (WITH BACKFILL)
# ==========================================
def get_gk2a_fires():
    print("📡 Connecting to GK-2A (Real-Time AWS)...")
    fires = []
    
    try:
        s3 = boto3.client('s3', region_name='us-east-1', config=Config(signature_version=UNSIGNED))
        bucket = 'noaa-gk2a-pds'
        
        now = datetime.utcnow()
        
        def list_files(dt):
            prefix = f"GK2A/AMI/L1B/IR038/{dt.strftime('%Y%m')}/{dt.strftime('%d')}/{dt.strftime('%H')}/"
            return s3.list_objects_v2(Bucket=bucket, Prefix=prefix)

        # Attempt 1
        resp = list_files(now)
        
        # Attempt 2 (Backfill)
        if 'Contents' not in resp:
            print(f"   ⚠️ No data for {now.strftime('%H')}:00. Checking backfill...")
            prev_hour = now - timedelta(hours=1)
            resp = list_files(prev_hour)

        if 'Contents' not in resp:
            print("   ❌ GK-2A Blind: No Data on AWS for last 2 hours.")
            return []
            
        latest_key = resp['Contents'][-1]['Key']
        filename = latest_key.split('/')[-1]
        print(f"   ⬇️ Downloading: {filename} (~50MB)...")
        
        s3.download_file(bucket, latest_key, "gk2a_temp.nc")
        
        import netCDF4
        import numpy as np
        ds = netCDF4.Dataset("gk2a_temp.nc")
        
        data_var = None
        for v in ['image_pixel_values', 'brightness_temperature']:
            if v in ds.variables:
                data_var = ds.variables[v]
                break
        
        if data_var is None:
            return []

        raw_data = data_var[:] 
        scale = getattr(data_var, 'scale_factor', 1.0)
        offset = getattr(data_var, 'add_offset', 0.0)
        temps = raw_data * scale + offset
        
        india_sector = temps[800:2500, 500:2000] 
        hot_indices = np.argwhere(india_sector > 312.0)
        
        print(f"   🔥 Thermal Anomalies: {len(hot_indices)}")
        
        for y, x in hot_indices:
            real_y = y + 800
            real_x = x + 500
            lat = 40.0 - (real_y * 0.016) 
            lon = 70.0 + (real_x * 0.016)
            temp_k = india_sector[y, x]
            
            if (20.0 < lat < 33.0) and (70.0 < lon < 90.0):
                fires.append({
                    'latitude': round(float(lat), 3),
                    'longitude': round(float(lon), 3),
                    'frp': round((float(temp_k) - 300.0) * 2.0, 1),
                    'source': 'GK2A (Real)',
                    'conf_score': 'EagleEye'
                })
        
        ds.close()
        if os.path.exists("gk2a_temp.nc"): os.remove("gk2a_temp.nc")
        
    except Exception as e:
        print(f"   ⚠️ GK-2A Error: {e}")
    
    return fires
    
    
# ==========================================
# 🧠 SMART DATABASE
# ==========================================
def save_fire_event_smart(lat, lon, source, cluster_size, region, frp, confidence):
    search_radius = 0.025 if "GK2A" in source else 0.001
    lat_min, lat_max = lat - search_radius, lat + search_radius
    lon_min, lon_max = lon - search_radius, lon + search_radius
    
    check_url = f"{SUPABASE_URL}/rest/v1/fires?lat=gte.{lat_min}&lat=lte.{lat_max}&lon=gte.{lon_min}&lon=lte.{lon_max}&select=*"
    
    try:
        r = db_session.get(check_url, timeout=10)
        existing_fires = r.json() if r.status_code == 200 else []
        
        match = None
        for f in existing_fires:
            dist_km = math.sqrt((f['lat'] - lat)**2 + (f['lon'] - lon)**2) * 111
            limit_km = 2.5 if "GK2A" in source else 0.1
            if dist_km < limit_km: 
                match = f
                break
        
        now_time = datetime.utcnow().isoformat()
        
        def safe_float(val):
            try:
                f_val = float(val)
                if math.isnan(f_val) or math.isinf(f_val): return 0.0
                return f_val
            except: return 0.0

        clean_frp = safe_float(frp)
        clean_lat = safe_float(lat)
        clean_lon = safe_float(lon)

        if match:
            print(f"   🔄 Merging with Event {match['id']}...")
            row_id = match['id']
            old_src = match.get('source', '')
            new_source = old_src if source in old_src else f"{old_src}, {source}"
            new_frp = max(float(match.get('frp_mw', 0)), clean_frp)
            
            payload = {
                "last_seen": now_time,
                "frp_mw": new_frp,
                "source": new_source,
                "alert_count": match.get('alert_count', 1) + 1
            }
            db_session.patch(f"{SUPABASE_URL}/rest/v1/fires?id=eq.{row_id}", json=payload, timeout=10)
            return False 
        else:
            pixel_size = 2000000 if "GK2A" in source else 375000
            est_area = (pixel_size * cluster_size) * 0.15
            
            payload = {
                "lat": clean_lat,
                "lon": clean_lon,
                "first_seen": now_time, 
                "last_seen": now_time,
                "source": source,
                "alert_count": 1,
                "location": region, 
                "frp_mw": clean_frp,
                "confidence": str(confidence),
                "est_area_m2": safe_float(est_area)
            }
            
            r_post = db_session.post(f"{SUPABASE_URL}/rest/v1/fires", json=payload, timeout=10)
            if r_post.status_code not in [200, 201]:
                print(f"   ❌ UPLOAD FAILED: {r_post.status_code}")
                print(f"   ⚠️ REASON: {r_post.text}")
            
            return True 

    except Exception as e:
        print(f"DB Error: {e}")
        time.sleep(1) 
        return False

# ==========================================
# 🤖 AI ANALYST & BROADCAST SYSTEM
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

def send_telegram_broadcast(msg):
    """
    Broadcasting Engine: Iterates through GitHub Secret IDs
    """
    print(f"🚀 Broadcasting Alert to {len(RECIPIENT_LIST)} targets...")
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    
    for user_id in RECIPIENT_LIST:
        try:
            requests.post(url, json={"chat_id": user_id, "text": msg}, timeout=5)
            print(f"   ✅ Sent to ID ending in ...{str(user_id)[-3:]}")
        except Exception as e:
            print(f"   ❌ Failed to send to ...{str(user_id)[-3:]}: {e}")

# ==========================================
# 🚀 SATARK V13.4 (24/7 SENTINEL)
# ==========================================
def scan_sector():
    print(f"\n🚀 SATARK V13.4 SENTINEL | {datetime.now().strftime('%H:%M:%S')}")
    all_fires = []

    # 1. POLAR (NASA)
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

    # 2. GEO (GK-2A)
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
        
        land_type = verify_land_use(lat, lon, region, frp)
        if land_type in ["INDUSTRY", "WATER"]: continue
        if frp > 500 and land_type != "FARM": continue

        is_new = save_fire_event_smart(lat, lon, source, 1, region, frp, f.get('conf_score', 'Low'))
        
        if is_new:
            if region == "WEST_BENGAL" or frp > 50.0:
                print(f"   🔥 ALERT: {region} | {frp}MW")
                ai_msg = analyze_with_ai(lat, lon, region, frp)
                if "FOREIGN" not in ai_msg:
                    msg = f"🔥 SATARK ALERT\n📍 {region}\n⚡ {frp:.1f} MW\n🤖 {ai_msg}\n🔗 https://maps.google.com/?q={lat},{lon}"
                    send_telegram_broadcast(msg)

if __name__ == "__main__":
    scan_sector()
        s3 = boto3.client('s3', region_name='us-east-1', config=Config(signature_version=UNSIGNED))
        bucket = 'noaa-gk2a-pds'
        
        # Logic: Check Current Hour -> If empty, Check Previous Hour
        now = datetime.utcnow()
        
        def list_files(dt):
            prefix = f"GK2A/AMI/L1B/IR038/{dt.strftime('%Y%m')}/{dt.strftime('%d')}/{dt.strftime('%H')}/"
            return s3.list_objects_v2(Bucket=bucket, Prefix=prefix)

        # Attempt 1
        resp = list_files(now)
        
        # Attempt 2 (Backfill)
        if 'Contents' not in resp:
            print(f"   ⚠️ No data for {now.strftime('%H')}:00. Checking backfill...")
            prev_hour = now - timedelta(hours=1)
            resp = list_files(prev_hour)

        if 'Contents' not in resp:
            print("   ❌ GK-2A Blind: No Data on AWS for last 2 hours.")
            return []
            
        latest_key = resp['Contents'][-1]['Key']
        filename = latest_key.split('/')[-1]
        print(f"   ⬇️ Downloading: {filename} (~50MB)...")
        
        s3.download_file(bucket, latest_key, "gk2a_temp.nc")
        
        import netCDF4
        import numpy as np
        ds = netCDF4.Dataset("gk2a_temp.nc")
        
        data_var = None
        for v in ['image_pixel_values', 'brightness_temperature']:
            if v in ds.variables:
                data_var = ds.variables[v]
                break
        
        if data_var is None:
            return []

        raw_data = data_var[:] 
        scale = getattr(data_var, 'scale_factor', 1.0)
        offset = getattr(data_var, 'add_offset', 0.0)
        temps = raw_data * scale + offset
        
        india_sector = temps[800:2500, 500:2000] 
        hot_indices = np.argwhere(india_sector > 312.0)
        
        print(f"   🔥 Thermal Anomalies: {len(hot_indices)}")
        
        for y, x in hot_indices:
            real_y = y + 800
            real_x = x + 500
            lat = 40.0 - (real_y * 0.016) 
            lon = 70.0 + (real_x * 0.016)
            temp_k = india_sector[y, x]
            
            if (20.0 < lat < 33.0) and (70.0 < lon < 90.0):
                fires.append({
                    'latitude': round(float(lat), 3),
                    'longitude': round(float(lon), 3),
                    'frp': round((float(temp_k) - 300.0) * 2.0, 1),
                    'source': 'GK2A (Real)',
                    'conf_score': 'EagleEye'
                })
        
        ds.close()
        if os.path.exists("gk2a_temp.nc"): os.remove("gk2a_temp.nc")
        
    except Exception as e:
        print(f"   ⚠️ GK-2A Error: {e}")
    
    return fires

# ==========================================
# 🧠 SMART DATABASE (SANITIZED & DEBUGGED)
# ==========================================
def save_fire_event_smart(lat, lon, source, cluster_size, region, frp, confidence):
    # 1. SETUP SESSION & SEARCH
    search_radius = 0.025 if "GK2A" in source else 0.001
    lat_min, lat_max = lat - search_radius, lat + search_radius
    lon_min, lon_max = lon - search_radius, lon + search_radius
    
    check_url = f"{SUPABASE_URL}/rest/v1/fires?lat=gte.{lat_min}&lat=lte.{lat_max}&lon=gte.{lon_min}&lon=lte.{lon_max}&select=*"
    
    try:
        # 2. CHECK EXISTING (GET)
        r = db_session.get(check_url, timeout=10)
        existing_fires = r.json() if r.status_code == 200 else []
        
        match = None
        for f in existing_fires:
            dist_km = math.sqrt((f['lat'] - lat)**2 + (f['lon'] - lon)**2) * 111
            limit_km = 2.5 if "GK2A" in source else 0.1
            if dist_km < limit_km: 
                match = f
                break
        
        now_time = datetime.utcnow().isoformat()
        
        # 🛡️ THE SANITIZER (The Fix for 400 Errors)
        # Prevents "NaN" from crashing Supabase
        def safe_float(val):
            try:
                f_val = float(val)
                if math.isnan(f_val) or math.isinf(f_val): return 0.0
                return f_val
            except: return 0.0

        clean_frp = safe_float(frp)
        clean_lat = safe_float(lat)
        clean_lon = safe_float(lon)

        if match:
            # === MERGE ===
            print(f"   🔄 Merging with Event {match['id']}...")
            row_id = match['id']
            old_src = match.get('source', '')
            new_source = old_src if source in old_src else f"{old_src}, {source}"
            # Ensure we don't overwrite a good reading with 0.0
            new_frp = max(float(match.get('frp_mw', 0)), clean_frp)
            
            payload = {
                "last_seen": now_time,
                "frp_mw": new_frp,
                "source": new_source,
                "alert_count": match.get('alert_count', 1) + 1
            }
            db_session.patch(f"{SUPABASE_URL}/rest/v1/fires?id=eq.{row_id}", json=payload, timeout=10)
            return False 
        else:
            # === INSERT ===
            pixel_size = 2000000 if "GK2A" in source else 375000
            est_area = (pixel_size * cluster_size) * 0.15
            
            payload = {
                "lat": clean_lat,
                "lon": clean_lon,
                "first_seen": now_time, 
                "last_seen": now_time,
                "source": source,
                "alert_count": 1,
                "location": region, 
                "frp_mw": clean_frp,
                "confidence": str(confidence),
                "est_area_m2": safe_float(est_area)
            }
            
            # POST with Error Reporting
            r_post = db_session.post(f"{SUPABASE_URL}/rest/v1/fires", json=payload, timeout=10)
            
            # 🚨 IF THIS FAILS, PRINT WHY
            if r_post.status_code not in [200, 201]:
                print(f"   ❌ UPLOAD FAILED: {r_post.status_code}")
                print(f"   ⚠️ REASON: {r_post.text}") # <--- This will tell us the exact error
            
            return True 

    except Exception as e:
        print(f"DB Error: {e}")
        time.sleep(1) 
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

    # 1. POLAR (NASA)
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

    # 2. GEO (GK-2A)
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
        
        land_type = verify_land_use(lat, lon, region, frp)
        if land_type in ["INDUSTRY", "WATER"]: continue
        if frp > 500 and land_type != "FARM": continue

        is_new = save_fire_event_smart(lat, lon, source, 1, region, frp, f.get('conf_score', 'Low'))
        
        if is_new:
            if region == "WEST_BENGAL" or frp > 50.0:
                print(f"   🔥 ALERT: {region} | {frp}MW")
                ai_msg = analyze_with_ai(lat, lon, region, frp)
                if "FOREIGN" not in ai_msg:
                    msg = f"🔥 SATARK ALERT\n📍 {region}\n⚡ {frp:.1f} MW\n🤖 {ai_msg}\n🔗 https://maps.google.com/?q={lat},{lon}"
                    send_telegram(msg)

if __name__ == "__main__":
    scan_sector()





