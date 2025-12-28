import os
import pandas as pd
import matplotlib.pyplot as plt
import requests
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from datetime import datetime
from openai import OpenAI

# ==========================================
# 🔐 CONFIGURATION
# ==========================================
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
OPENROUTER_API_KEY = os.environ.get("OPENROUTER_API_KEY") # <--- Llama needs this
SENDER_EMAIL = os.environ.get("SENDER_EMAIL")
GMAIL_PASSWORD = os.environ.get("GMAIL_APP_PASSWORD")
RECIPIENT_EMAIL = "reezaalarafat@gmail.com"

# AI Client (OpenRouter)
client = OpenAI(
    base_url="https://openrouter.ai/api/v1",
    api_key=OPENROUTER_API_KEY,
)

def fetch_monthly_data():
    print("📡 Fetching Monthly Data from Supabase...")
    headers = {"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}"}
    
    # Get 1st day of current month
    today = datetime.now()
    first_day = today.replace(day=1, hour=0, minute=0, second=0).isoformat()
    
    # Fetch all fires active this month
    url = f"{SUPABASE_URL}/rest/v1/fires?select=*&last_seen=gte.{first_day}"
    
    try:
        r = requests.get(url, headers=headers)
        data = r.json()
        return pd.DataFrame(data) if data else pd.DataFrame()
    except Exception as e:
        print(f"❌ DB Error: {e}")
        return pd.DataFrame()

def apply_thesis_physics(df):
    """
    CONVERTS SATELLITE WATTS -> REAL WORLD EMISSIONS
    Citation: Wooster et al. (2005), Andreae (2019)
    """
    if df.empty: return df
    
    # 1. DATA CLEANING
    if 'frp_mw' not in df.columns: df['frp_mw'] = 0.0
    
    # 2. PHYSICS CALCULATION
    # Energy (MJ) = MW * Duration (Assuming 1 hr snapshot = 3600s)
    df['energy_mj'] = df['frp_mw'] * 3600 
    
    # Biomass Burnt (Tonnes) = MJ * 0.368 (Combustion Coefficient) / 1000
    df['biomass_tonnes'] = (df['energy_mj'] * 0.368) / 1000
    
    # 3. EMISSION ESTIMATES (Ag Residue Factors)
    df['co2_tonnes'] = df['biomass_tonnes'] * 1.585
    df['pm25_kg'] = df['biomass_tonnes'] * 6.26
    
    return df

def get_ai_report(stats):
    """
    Uses Llama 3.3 to write a Thesis-Grade Ecological Report.
    """
    try:
        prompt = f"""
        ACT AS: Senior Atmospheric Physicist & Ecologist.
        
        INPUT DATA (West Bengal Sector, {datetime.now().strftime('%B %Y')}):
        - Total Fire Events: {stats['count']}
        - Peak Thermal Intensity: {stats['max_mw']:.1f} MW
        - Estimated Biomass Incinerated: {stats['biomass']:.1f} Tonnes
        - CO2 Injection (Atmosphere): {stats['co2']:.1f} Tonnes
        - PM2.5 Aerosol Load (Toxic Smog): {stats['pm25']:.1f} kg
        
        TASK: Write a "Monthly Strategic Ecological Impact Report" (Markdown).
        
        SECTIONS REQUIRED:
        1. **Executive Summary**: High-level overview of the fire season severity based on the data.
        2. **Atmospheric Toxicity & Public Health**: 
           - Analyze the PM2.5 load ({stats['pm25']:.1f} kg). 
           - Explain the health risks (respiratory/cardiovascular) to the local population.
           - Mention "Aerosol Optical Depth (AOD)" implications.
        3. **Ecological Damage Assessment**: 
           - Discuss the carbon footprint ({stats['co2']:.1f} Tonnes CO2).
           - Mention soil degradation (loss of Nitrogen/Phosphorus) due to stubble burning.
        4. **Strategic Intervention**: 
           - Suggest 2 science-backed solutions (e.g., Happy Seeder technology, Bio-decomposers) to reduce next month's numbers.
        
        TONE: Scientific, Urgent, Data-Driven. Use bullet points.
        """
        
        # SWITCHING TO LLAMA 3.3
        completion = client.chat.completions.create(
            model="meta-llama/llama-3.3-70b-instruct:free",
            messages=[{"role": "user", "content": prompt}]
        )
        return completion.choices[0].message.content
    except Exception as e:
        print(f"AI Error: {e}")
        return "AI Analysis Unavailable due to API limits."

def send_email(report_path, map_path, csv_path):
    if not SENDER_EMAIL: return
    
    msg = MIMEMultipart()
    msg['From'] = SENDER_EMAIL
    msg['To'] = RECIPIENT_EMAIL
    msg['Subject'] = f"🚨 SATARK ECOLOGICAL AUDIT: {datetime.now().strftime('%B %Y')}"
    msg.attach(MIMEText("Attached: Monthly Ecological Damage Report & Raw Thesis Data.", 'plain'))
    
    # Attach Map, Report, and CSV
    for fpath in [report_path, map_path, csv_path]:
        if os.path.exists(fpath):
            with open(fpath, "rb") as f:
                part = MIMEBase("application", "octet-stream")
                part.set_payload(f.read())
                encoders.encode_base64(part)
                part.add_header("Content-Disposition", f"attachment; filename={os.path.basename(fpath)}")
                msg.attach(part)
    
    try:
        with smtplib.SMTP("smtp.gmail.com", 587) as s:
            s.starttls()
            s.login(SENDER_EMAIL, GMAIL_PASSWORD)
            s.send_message(msg)
        print("✅ Audit Email Sent.")
    except Exception as e:
        print(f"❌ Email Error: {e}")

def run_audit():
    print(f"🚀 SATARK MONTHLY AUDIT (LLAMA EDITION) | {datetime.now().strftime('%Y-%m')}")
    
    # 1. FETCH & CALCULATE
    df = fetch_monthly_data()
    if df.empty:
        print("⚠️ No data found for this month.")
        return

    df = apply_thesis_physics(df)
    
    # 2. GENERATE MAP (Heat Intensity)
    plt.figure(figsize=(10, 8), facecolor='#121212')
    ax = plt.axes(); ax.set_facecolor("#121212")
    
    # Plot: X=Lon, Y=Lat, Color=Watts
    sc = plt.scatter(df['lon'], df['lat'], c=df['frp_mw'], cmap='inferno', s=60, alpha=0.8)
    plt.colorbar(sc, label='Fire Radiative Power (MW)')
    plt.title(f"THERMAL INTENSITY MAP | {datetime.now().strftime('%B %Y')}", color='white')
    
    os.makedirs('audit_out', exist_ok=True)
    map_file = "audit_out/intensity_map.png"
    plt.savefig(map_file, dpi=150, bbox_inches='tight')
    plt.close()

    # 3. GENERATE REPORT
    stats = {
        'count': len(df),
        'max_mw': df['frp_mw'].max(),
        'biomass': df['biomass_tonnes'].sum(),
        'co2': df['co2_tonnes'].sum(),
        'pm25': df['pm25_kg'].sum()
    }
    print("🤖 Consulting Llama 3.3 for Ecological Analysis...")
    ai_text = get_ai_report(stats)
    
    report_file = "audit_out/report.md"
    with open(report_file, "w") as f:
        f.write(f"# SATARK ECOLOGICAL DAMAGE ASSESSMENT\n\n{ai_text}")

    # 4. EXPORT DATA (For your Thesis backup)
    csv_file = f"audit_out/data_{datetime.now().strftime('%Y-%m')}.csv"
    df.to_csv(csv_file, index=False)

    # 5. DISPATCH
    send_email(report_file, map_file, csv_file)
    print("✅ Audit Complete.")

if __name__ == "__main__":
    run_audit()
