import requests
import pandas as pd
import os
import time
import io
from datetime import datetime
from openai import OpenAI 

# ==========================================
# 🔐 CONFIGURATION
# ==========================================
try:
    # 1. FETCH & SANITIZE KEYS
    NASA_KEY_RAW = os.environ.get("NASA_KEY", "")
    NASA_KEY = NASA_KEY_RAW.strip()
    
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

# 🌍 INDIA BOUNDING BOX (West, South, East, North)
INDIA_BOX = "68,6,98,38"

# ==========================================
# 🗺️ REGION INTELLIGENCE
# ==========================================
def get_region_tag(lat, lon):
    # WEST BENGAL (Approx)
    if 21.5 <= lat <= 27.3 and 85.8 <= lon <= 89.9: return "WEST_BENGAL"
    # PUNJAB & HARYANA (Stubble Belt)
    if 28.4 <= lat <= 32.5 and 73.8 <= lon <= 77.8: return "PUNJAB_HARYANA"
    # CENTRAL INDIA
    if 21.0 <= lat <= 26.0 and 74.0 <= lon <= 84.0: return "CENTRAL_INDIA"
    # SOUTH INDIA
    if 8.0 <= lat <= 20.0 and 74.0 <= lon <= 85.0: return "SOUTH_INDIA"
    
    return "INDIA_OTHER"

# ==========================================
# 🕵️‍♂️ OSM VERIFICATION (UPDATED V2)
# ==========================================
def verify_land_use(lat, lon, region, frp):
    # LOGIC UPDATE: If fire is HUGE (>50MW), verify it regardless of region.
    # Otherwise, save quota for Bengal only.
    if region != "WEST_BENGAL" and frp < 50.0: 
        return "UNVERIFIED (Quota Saved)"

    url = "http://overpass-api.de/api/interpreter"
    # QUERY UPDATE: Added checks for mines, quarries, and water (glint)
    query = f"""
    [out:json];
    (
      way(around:500, {lat}, {lon})["landuse"];
      way(around:500, {lat}, {lon})["industrial"];
      way(around:500, {lat}, {lon})["natural"="water"];
      node(around:500, {lat}, {lon})["man_made"="mineshaft"];
      way(around:500, {lat}, {lon})["landuse"="quarry"];
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
                
                # Check for Mines/Industry specific tags
                if 'industrial' in tags: return "INDUSTRY"
                if tags.get('man_made') == 'mineshaft': return "INDUSTRY" # Catch Mines
                if tags.get('landuse') == 'quarry': return "INDUSTRY" # Catch Open Cast
                if tags.get('natural') == 'water': return "WATER" # Catch Glint
                
                if 'landuse' in tags: tags_found.append(tags['landuse'])
        
        # General Categorization
        if any(t in ['industrial', 'residential', 'railway', 'brownfield'] for t in tags_found): return "INDUSTRY"
        if any(t in ['farmland', 'farm', 'forest', 'orchard', 'grass'] for t in tags_found): return "FARM"
        if any(t in ['reservoir', 'basin'] for t in tags_found): return "WATER"
        
        return "UNKNOWN"
    except:
        return "UNKNOWN"

# ==========================================
# 🧠 DATABASE LAYER
# ==========================================
def save_fire_event(lat, lon, source, cluster_size, region, total_frp):
    pixel_size = 1000000 if "MODIS" in source else 375000
    est_area = (pixel_size * cluster_size) * 0.15 
    
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json"
    }
    
    check_url = f"{SUPABASE_URL}/rest/v1/fires?select=*&lat=eq.{lat}&lon=eq.{lon}"
    
    try:
        r = requests.get(check_url, headers=headers)
        existing = r.json() if r.status_code == 200 else []
        now_time = datetime.utcnow().isoformat()
        
        if existing:
            row_id = existing[0]['id']
            old_count = existing[0].get('alert_count', 1)
            old_frp = existing[0].get('frp_mw', 0)
            new_frp = max(float(old_frp), float(total_frp))
            
            payload = {
                "last_seen": now_time,
                "alert_count": old_count + 1,
                "frp_mw": new_frp,
                "est_area_m2": float(est_area)
            }
            requests.patch(f"{SUPABASE_URL}/rest/v1/fires?id=eq.{row_id}", headers=headers, json=payload)
            return False, est_area 
        else:
            payload = {
                "lat": float(lat),
                "lon": float(lon),
                "first_seen": now_time,
                "last_seen": now_time,
                "source": source,
                "alert_count": 1,
                "est_area_m2": float(est_area),
                "location": region, 
                "frp_mw": float(total_frp)
            }
            requests.post(f"{SUPABASE_URL}/rest/v1/fires", headers=headers, json=payload)
            return True, est_area 

    except Exception:
        return False, est_area

# ==========================================
# 🤖 AI BORDER GUARD (POWERED BY LLAMA 3)
# ==========================================
def analyze_with_ai(lat, lon, region, frp):
    try:
        # 🛡️ THE BORDER PATROL PROMPT
        prompt = f"""
        TASK: Geolocation & Threat Analysis.
        COORDS: {lat}, {lon}
        REGION TAG: {region}
        ENERGY: {frp:.1f} MW
        
        INSTRUCTIONS:
        1. CRITICAL: Check if these coordinates are strictly inside INDIA.
        2. If the location is in BANGLADESH, PAKISTAN, NEPAL or BHUTAN -> Output exactly: "FOREIGN_FIRE"
        3. If inside INDIA -> Output a short, urgent 10-word alert describing the fire location and intensity.
        """
        
        completion = client.chat.completions.create(
            # Using Llama 3 70B for superior reasoning on geography
            model="meta-llama/llama-3-70b-instruct", 
            messages=[{"role": "user", "content": prompt}]
        )
        return completion.choices[0].message.content.strip()
    except:
        return "Confirmed Heat Signature."

def send_telegram(msg):
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        requests.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "text": msg})
    except: pass

# ==========================================
# 🛰️ SATARK V12.2 (LLAMA 3 ENGINE)
# ==========================================
def scan_sector():
    print(f"\n🚀 SATARK V12.2 (LLAMA POWERED) | {datetime.now().strftime('%H:%M:%S')}")
    
    # 🚨 CRITICAL: USING AREA API
    base_url = "https://firms.modaps.eosdis.nasa.gov/api/area/csv"
    
    satellites = {
        "VIIRS_SNPP": f"{base_url}/{NASA_KEY}/VIIRS_SNPP_NRT/{INDIA_BOX}/3",
        "VIIRS_NOAA": f"{base_url}/{NASA_KEY}/VIIRS_NOAA20_NRT/{INDIA_BOX}/3",
        "MODIS": f"{base_url}/{NASA_KEY}/MODIS_NRT/{INDIA_BOX}/3"
    }
    
    all_fires = []

    for sat_name, url in satellites.items():
        print(f"📡 Scanning {sat_name}...", end=" ")
        try:
            r = requests.get(url, timeout=45) 
            
            if r.status_code != 200:
                print(f"❌ API Error {r.status_code}")
                continue
            
            try:
                df = pd.read_csv(io.StringIO(r.text))
            except:
                print("⚠️ Bad Data"); continue

            df.columns = [c.lower() for c in df.columns]
            
            if 'latitude' not in df.columns:
                print("✅ 0 Fires"); continue

            if 'frp' not in df.columns:
                col = 'power' if 'power' in df.columns else 'brightness'
                df['frp'] = df[col] if col in df.columns else 0.0
            
            if 'confidence' in df.columns:
                df = df[df['confidence'].astype(str) != 'l']

            if not df.empty:
                df['source'] = sat_name
                all_fires.append(df)
                print(f"✅ {len(df)} Points")

        except Exception as e: print(f"❌ Error: {e}")

    if not all_fires:
        print("✅ India Sector Clear.")
        return

    # PROCESS
    merged = pd.concat(all_fires)
    merged['lat_r'] = merged['latitude'].round(2)
    merged['lon_r'] = merged['longitude'].round(2)
    
    clusters = merged.groupby(['lat_r', 'lon_r']).agg({
        'latitude': 'mean', 'longitude': 'mean', 
        'source': 'first', 'frp': 'sum', 'lat_r': 'count'
    }).rename(columns={'lat_r': 'size'}).reset_index()

    print(f"📊 Analyzing {len(clusters)} Events...")

    fire_count = 0

    for _, f in clusters.iterrows():
        lat, lon = f['latitude'], f['longitude']
        frp = f['frp']
        region = get_region_tag(lat, lon)
        
        # 1. VERIFY LAND USE (Now with FRP check)
        land_type = verify_land_use(lat, lon, region, frp)
        
        # 2. FILTER INDUSTRIAL / WATER
        if land_type == "INDUSTRY": 
            print(f"📉 Filtered Industrial Heat at {lat},{lon}")
            continue
        if land_type == "WATER":
            print(f"📉 Filtered Sun Glint at {lat},{lon}")
            continue

        # 3. GLINT / ANOMALY CHECK (The 300MW Safety Bumper)
        # If fire is MASSIVE (>300MW) and NOT confirmed Farm/Forest, kill it.
        # This catches "Ice Reflection" or random anomalies.
        if frp > 300.0 and land_type not in ["FARM", "FOREST", "UNKNOWN"]:
             print(f"⚠️ GLINT SUSPECTED: {frp:.1f}MW. Skipping.")
             continue

        # SAVE EVERYTHING
        is_new, area = save_fire_event(lat, lon, f['source'], f['size'], region, frp)
        
        if is_new:
            fire_count += 1
            
            # ALERT LOGIC
            should_alert = False
            if region == "WEST_BENGAL": should_alert = True 
            elif region == "PUNJAB_HARYANA" and frp > 50.0: should_alert = True
            elif frp > 100.0: should_alert = True
            
            if should_alert:
                print(f"   🔥 ANALYZING: {region} | {frp:.1f} MW")
                
                # LLAMA 3 CHECK
                ai_msg = analyze_with_ai(lat, lon, region, frp)
                
                if "FOREIGN_FIRE" in ai_msg:
                    print(f"   🚫 SKIPPED (Llama detected Foreign Fire)")
                    continue
                
                state_emoji = "🚜" if "PUNJAB" in region else "🌾" if "BENGAL" in region else "🇮🇳"
                msg = (f"{state_emoji} SATARK INDIA\n📍 {region}\n"
                       f"🔥 {frp:.1f} MW | 🤖 {ai_msg}\n"
                       f"🔗 https://maps.google.com/?q={lat},{lon}")
                send_telegram(msg)

    print(f"✅ Cycle Complete. {fire_count} New Fires Logged.")

if __name__ == "__main__":
    scan_sector()

