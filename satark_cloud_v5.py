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
    NASA_KEY = os.environ.get("NASA_KEY")
    TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
    TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")
    SUPABASE_URL = os.environ.get("SUPABASE_URL")
    SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
    OPENROUTER_API_KEY = os.environ.get("OPENROUTER_API_KEY") 
    
    if not all([NASA_KEY, TELEGRAM_BOT_TOKEN, SUPABASE_URL, SUPABASE_KEY, OPENROUTER_API_KEY]):
        print("⚠️ CRITICAL: Missing Keys.")
except Exception as e:
    print(f"Config Error: {e}")

client = OpenAI(
    base_url="https://openrouter.ai/api/v1",
    api_key=OPENROUTER_API_KEY,
)

# ==========================================
# 🗺️ REGION INTELLIGENCE
# ==========================================
def get_region_tag(lat, lon):
    """
    Returns the State/Zone based on Lat/Lon boxes.
    """
    # WEST BENGAL (Approx)
    if 21.5 <= lat <= 27.3 and 85.8 <= lon <= 89.9:
        return "WEST_BENGAL"
    
    # PUNJAB & HARYANA (Stubble Belt)
    if 28.4 <= lat <= 32.5 and 73.8 <= lon <= 77.8:
        return "PUNJAB_HARYANA"
    
    # CENTRAL INDIA (MP/Chattisgarh)
    if 21.0 <= lat <= 26.0 and 74.0 <= lon <= 84.0:
        return "CENTRAL_INDIA"
        
    return "INDIA_OTHER"

# ==========================================
# 🕵️‍♂️ OSM VERIFICATION (Rate Limit Protected)
# ==========================================
def verify_land_use(lat, lon, region):
    """
    Only checks OSM for West Bengal to save API Quota.
    """
    if region != "WEST_BENGAL": 
        return "UNVERIFIED (Quota Saved)"

    url = "http://overpass-api.de/api/interpreter"
    query = f"""
    [out:json];
    (
      way(around:500, {lat}, {lon})["landuse"];
      way(around:500, {lat}, {lon})["industrial"];
    );
    out tags;
    """
    try:
        time.sleep(1) # Be nice to OSM
        r = requests.get(url, params={'data': query}, timeout=10)
        if r.status_code != 200: return "UNKNOWN"
        
        data = r.json()
        tags_found = []
        for el in data.get('elements', []):
            if 'tags' in el:
                tags = el['tags']
                if 'industrial' in tags: return "INDUSTRY"
                if 'landuse' in tags: tags_found.append(tags['landuse'])
        
        if any(t in ['industrial', 'residential', 'railway'] for t in tags_found): return "INDUSTRY"
        if any(t in ['farmland', 'farm', 'forest', 'orchard'] for t in tags_found): return "FARM"
        
        return "UNKNOWN"
    except:
        return "UNKNOWN"

# ==========================================
# 🧠 DATABASE LAYER
# ==========================================
def save_fire_event(lat, lon, source, cluster_size, region, total_frp):
    """
    Matches User Columns: lat, lon, first_seen, last_seen, source, alert_count, est_area_m2, location, frp_mw
    """
    pixel_size = 1000000 if "MODIS" in source else 375000
    est_area = (pixel_size * cluster_size) * 0.15 
    
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json"
    }
    
    # Check Existing (Deduplication)
    check_url = f"{SUPABASE_URL}/rest/v1/fires?select=*&lat=eq.{lat}&lon=eq.{lon}"
    
    try:
        r = requests.get(check_url, headers=headers)
        existing = r.json() if r.status_code == 200 else []
        
        now_time = datetime.utcnow().isoformat()
        
        if existing:
            # UPDATE
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
            # INSERT NEW
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
            r = requests.post(f"{SUPABASE_URL}/rest/v1/fires", headers=headers, json=payload)
            if r.status_code in [200, 201]:
                return True, est_area 
            else:
                return False, est_area

    except Exception:
        return False, est_area

# ==========================================
# 🤖 AI ANALYSIS & ALERTING
# ==========================================
def analyze_with_ai(lat, lon, region, frp):
    try:
        prompt = f"""
        ACT AS: Satellite Analyst.
        DATA: {region} | Energy: {frp:.1f} MW.
        OUTPUT: 10-word threat assessment.
        """
        completion = client.chat.completions.create(
            model="google/gemini-2.0-flash-exp:free", 
            messages=[{"role": "user", "content": prompt}]
        )
        return completion.choices[0].message.content.strip()
    except:
        return "Satellite Heat Signature Confirmed."

def send_telegram(msg):
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        requests.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "text": msg})
    except: pass

# ==========================================
# 🛰️ PAN-INDIA SCANNER
# ==========================================
def scan_sector():
    print(f"\n🚀 SATARK V10.0 (PAN-INDIA) | {datetime.now().strftime('%H:%M:%S')}")
    
    base_url = "https://firms.modaps.eosdis.nasa.gov/api/country/csv"
    satellites = {
        "VIIRS_SNPP": f"{base_url}/{NASA_KEY}/VIIRS_SNPP_NRT/IND/1",
        "VIIRS_NOAA": f"{base_url}/{NASA_KEY}/VIIRS_NOAA20_NRT/IND/1",
        "MODIS": f"{base_url}/{NASA_KEY}/MODIS_NRT/IND/1"
    }
    
    all_fires = []

    # 1. FETCH
    for sat_name, url in satellites.items():
        print(f"📡 Scanning {sat_name}...", end=" ")
        try:
            r = requests.get(url, timeout=30)
            if r.status_code != 200:
                print(f"❌ API Error"); continue
            
            try:
                df = pd.read_csv(io.StringIO(r.text))
            except:
                print("⚠️ Bad Data"); continue

            df.columns = [c.lower() for c in df.columns]
            if 'latitude' not in df.columns:
                print("✅ 0 Fires"); continue

            # MAP FRP
            if 'frp' not in df.columns:
                col = 'power' if 'power' in df.columns else 'brightness'
                df['frp'] = df[col] if col in df.columns else 0.0
            
            # FILTER CONFIDENCE (Basic cleanup)
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

    # 2. PROCESS WHOLE COUNTRY
    merged = pd.concat(all_fires)
    merged['lat_r'] = merged['latitude'].round(2)
    merged['lon_r'] = merged['longitude'].round(2)
    
    # Cluster nearby fires
    clusters = merged.groupby(['lat_r', 'lon_r']).agg({
        'latitude': 'mean', 'longitude': 'mean', 
        'source': 'first', 'frp': 'sum', 'lat_r': 'count'
    }).rename(columns={'lat_r': 'size'}).reset_index()

    print(f"📊 Analyzing {len(clusters)} Total India Events...")

    fire_count = 0
    
    for _, f in clusters.iterrows():
        lat, lon = f['latitude'], f['longitude']
        frp = f['frp']
        
        # 3. IDENTIFY REGION
        region = get_region_tag(lat, lon)
        
        # 4. OSM CHECK (Only for Bengal)
        land_type = verify_land_use(lat, lon, region)
        if land_type == "INDUSTRY": continue

        # 5. SAVE TO DB (All Valid Fires)
        is_new, area = save_fire_event(lat, lon, f['source'], f['size'], region, frp)
        
        if is_new:
            fire_count += 1
            
            # 6. SMART ALERTING (The Spam Filter)
            should_alert = False
            
            if region == "WEST_BENGAL":
                should_alert = True # Alert EVERYTHING in Bengal
            elif region == "PUNJAB_HARYANA" and frp > 50.0:
                should_alert = True # Only Alert BIG fires in Punjab
            elif frp > 100.0:
                should_alert = True # Alert MASSIVE fires anywhere in India
            
            if should_alert:
                print(f"   🔥 ALERT: {region} | {frp:.1f} MW")
                ai_msg = analyze_with_ai(lat, lon, region, frp)
                
                # Dynamic Emoji based on State
                state_emoji = "🚜" if "PUNJAB" in region else "🌾" if "BENGAL" in region else "🇮🇳"
                
                msg = (f"{state_emoji} SATARK INDIA ALERT\n"
                       f"📍 {region}\n"
                       f"🔥 {frp:.1f} MW Intensity\n"
                       f"🤖 {ai_msg}\n"
                       f"🔗 https://maps.google.com/?q={lat},{lon}")
                send_telegram(msg)

    print(f"✅ Cycle Complete. {fire_count} New Fires Logged.")

if __name__ == "__main__":
    scan_sector()
