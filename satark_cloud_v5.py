import requests
import pandas as pd
import os
import time
from datetime import datetime
from openai import OpenAI 

# ==========================================
# 🔐 CONFIGURATION & SETUP
# ==========================================
try:
    # API KEYS
    NASA_KEY = os.environ.get("NASA_KEY")
    TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
    TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")
    SUPABASE_URL = os.environ.get("SUPABASE_URL")
    SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
    OPENROUTER_API_KEY = os.environ.get("OPENROUTER_API_KEY") 
    
    if not all([NASA_KEY, TELEGRAM_BOT_TOKEN, SUPABASE_URL, SUPABASE_KEY, OPENROUTER_API_KEY]):
        print("⚠️ CRITICAL: Missing Keys. Check GitHub Secrets.")
except Exception as e:
    print(f"Config Error: {e}")

# AI CLIENT (Via OpenRouter)
client = OpenAI(
    base_url="https://openrouter.ai/api/v1",
    api_key=OPENROUTER_API_KEY,
)

# 🌍 WEST BENGAL GEO-FENCE
WB_LAT_MIN, WB_LAT_MAX = 21.5, 27.3
WB_LON_MIN, WB_LON_MAX = 85.8, 89.9

# ==========================================
# 🕵️‍♂️ TRUTH LAYER: OPENSTREETMAP (OSM)
# ==========================================
def verify_land_use(lat, lon):
    """
    Queries OSM to verify if the fire is on VALID land (Farm/Forest) vs INVALID (Industry/City).
    """
    overpass_url = "http://overpass-api.de/api/interpreter"
    query = f"""
    [out:json];
    (
      way(around:500, {lat}, {lon})["landuse"];
      way(around:500, {lat}, {lon})["industrial"];
      node(around:500, {lat}, {lon})["place"];
    );
    out tags;
    """
    try:
        time.sleep(1) # Respect Rate Limits
        response = requests.get(overpass_url, params={'data': query}, timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            tags_found = []
            
            for element in data['elements']:
                if 'tags' in element:
                    tags = element['tags']
                    if 'landuse' in tags: tags_found.append(tags['landuse'])
                    if 'industrial' in tags: return "INDUSTRY" 
            
            # BLOCK LIST (False Positives)
            if any(t in ['industrial', 'residential', 'commercial', 'retail', 'quarry', 'mine', 'railway', 'construction'] for t in tags_found):
                return "INDUSTRY"
            
            # ALLOW LIST (Valid Fires)
            if any(t in ['farmland', 'farm', 'orchard', 'meadow', 'grass', 'vineyard'] for t in tags_found):
                return "FARM"
            if any(t in ['forest', 'wood', 'scrub', 'nature_reserve'] for t in tags_found):
                return "FOREST"
                
            return "UNKNOWN" # Assume valid rural land if no data
    except:
        return "UNKNOWN" # Fail-open
    
    return "UNKNOWN"

# ==========================================
# 🧠 DATABASE LAYER (Supabase RPC)
# ==========================================
def save_fire_event(lat, lon, source, cluster_size, region, total_frp):
    """
    Sends 'frp_in' (Watts) to Supabase for Thesis Data.
    """
    # Legacy Area Est (For Visuals only)
    base_pixel = 1000000 if "MODIS" in source else 375000 
    est_area = (base_pixel * cluster_size) * 0.15 

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
        "region_in": region,
        "frp_in": float(total_frp) # <--- SENDING WATTS
    }

    try:
        rpc_url = f"{SUPABASE_URL}/rest/v1/rpc/upsert_fire_cluster"
        response = requests.post(rpc_url, headers=headers, json=payload)
        
        if response.status_code == 200:
            result = response.json()
            if isinstance(result, list) and len(result) > 0:
                return (result[0].get('status') == 'new'), est_area
            return False, est_area
        return False, est_area
    except Exception as e:
        print(f"⚠️ Connection Error: {e}")
        return False, est_area

# ==========================================
# 🤖 AI ANALYSIS
# ==========================================
def analyze_with_ai(lat, lon, land_type, frp):
    try:
        intensity = "Low"
        if frp > 10: intensity = "Moderate"
        if frp > 50: intensity = "High"

        prompt = f"""
        ACT AS: Satellite Fire Analyst.
        DATA: {lat},{lon} | Type: {land_type} | Energy: {frp:.1f} MW ({intensity}).
        CONTEXT: West Bengal, India.
        
        DECISION RULES:
        1. IF Type is INDUSTRY/URBAN/MINE -> DECISION: NO.
        2. IF Type is FARM/FOREST -> DECISION: YES.
        3. IF UNKNOWN & Energy > 2.0 MW -> DECISION: YES.
        4. IF UNKNOWN & Energy < 2.0 MW -> DECISION: NO (Likely noise).
        
        OUTPUT FORMAT: DECISION: [YES/NO] | MSG: [Urgent 10-word alert]
        """
        
        completion = client.chat.completions.create(
            model="google/gemini-2.0-flash-exp:free", 
            messages=[{"role": "user", "content": prompt}]
        )
        return completion.choices[0].message.content.strip()
    except:
        return "DECISION: YES | MSG: AI Offline. Heat signature confirmed."

def send_telegram(message):
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        requests.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "text": message})
    except: pass

# ==========================================
# 🛰️ MAIN SCANNER ENGINE
# ==========================================
def scan_sector():
    print(f"\n🚀 SATARK V9.0 (FINAL) | {datetime.now().strftime('%H:%M:%S')}")
    
    # 1. FETCH ALL INDIA DATA (VIIRS + MODIS)
    base_url = "https://firms.modaps.eosdis.nasa.gov/api/country/csv"
    
    satellites = {
        "VIIRS_SNPP": f"{base_url}/{NASA_KEY}/VIIRS_SNPP_NRT/IND/1",
        "VIIRS_NOAA": f"{base_url}/{NASA_KEY}/VIIRS_NOAA20_NRT/IND/1",
        "MODIS": f"{base_url}/{NASA_KEY}/MODIS_NRT/IND/1"
    }
    
    wb_fires = []

    for sat_name, url in satellites.items():
        try:
            print(f"📡 Scanning {sat_name}...", end=" ")
            try: 
                df = pd.read_csv(url)
                print(f"✅ {len(df)} Points Found")
            except: 
                print("❌ Download Failed")
                continue

            # NORMALIZE COLUMNS (Critical Fix)
            df.columns = [c.lower() for c in df.columns]
            
            # Map MODIS 'power' or 'brightness' to 'frp'
            if 'frp' not in df.columns:
                if 'power' in df.columns: df['frp'] = df['power']
                elif 'brightness' in df.columns: df['frp'] = df['brightness'] # Fallback
                else: df['frp'] = 0.0

            # 2. FILTER: GEOGRAPHY (West Bengal Only)
            local_df = df[
                (df['latitude'] >= WB_LAT_MIN) & (df['latitude'] <= WB_LAT_MAX) &
                (df['longitude'] >= WB_LON_MIN) & (df['longitude'] <= WB_LON_MAX)
            ].copy()
            
            # 3. FILTER: CONFIDENCE (Reject Low Quality)
            if 'confidence' in local_df.columns:
                # Remove strings 'l' (low) or numeric < 50
                local_df = local_df[local_df['confidence'].astype(str) != 'l']
                # If numeric column, filter < 30
                if pd.api.types.is_numeric_dtype(local_df['confidence']):
                    local_df = local_df[local_df['confidence'] > 30]

            if not local_df.empty:
                local_df['source'] = sat_name
                wb_fires.append(local_df)
                print(f"   ⚠️ {len(local_df)} Active Fires in WB Sector.")

        except Exception as e: print(f"Error: {e}")

    if not wb_fires:
        print("✅ West Bengal Sector Clear.")
        return

    # 4. CLUSTERING & ENERGY SUMMATION
    merged = pd.concat(wb_fires)
    merged['lat_r'] = merged['latitude'].round(2) 
    merged['lon_r'] = merged['longitude'].round(2)
    
    unique_clusters = merged.groupby(['lat_r', 'lon_r']).agg({
        'latitude': 'mean', 
        'longitude': 'mean', 
        'source': 'first', 
        'frp': 'sum',      # SUM THE WATTS (Thesis Math)
        'latitude': 'count'
    }).rename(columns={'latitude': 'cluster_size'}).reset_index()

    print(f"📊 Validating {len(unique_clusters)} Events...")

    for _, fire in unique_clusters.iterrows():
        lat = fire['latitude']
        lon = fire['longitude']
        source = fire['source']
        size = fire['cluster_size']
        total_frp = fire['frp']

        # 5. TRUTH VERIFICATION (OSM)
        land_type = verify_land_use(lat, lon)
        
        if land_type == "INDUSTRY":
            print(f"   🛑 BLOCKED: Industrial heat at {lat}, {lon}")
            continue

        # Region Logic
        region = "ZONE_UNKNOWN"
        if 21.5 <= lat <= 23.0: region = "ZONE_C_SOUTH"
        elif 23.0 < lat <= 25.0: region = "ZONE_D_CENTRAL"
        elif lat > 26.0: region = "ZONE_A_NORTH"

        # 6. SAVE TO DB
        is_new, area_est = save_fire_event(lat, lon, source, size, region, total_frp)

        if not is_new: continue 

        # 7. AI ALERT
        print(f"   🔥 ALERTING: {total_frp:.1f} MW | {land_type}")
        
        ai_res = analyze_with_ai(lat, lon, land_type, total_frp)
        
        if "DECISION: YES" in ai_res:
             msg_body = ai_res.split("MSG:")[1].strip() if "MSG:" in ai_res else "Fire Detected"
             telegram_msg = (
                 f"🔥 SATARK ALERT (WB)\n"
                 f"📍 {lat:.4f}, {lon:.4f} ({region})\n"
                 f"⚡ ENERGY: {total_frp:.1f} MW\n"
                 f"🌍 TYPE: {land_type}\n"
                 f"🤖 AI: {msg_body}\n"
                 f"🔗 https://maps.google.com/?q={lat},{lon}"
             )
             send_telegram(telegram_msg)

if __name__ == "__main__":
    scan_sector()
