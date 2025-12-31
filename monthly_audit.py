import os
import pandas as pd
import matplotlib.pyplot as plt
import requests
import smtplib
import time
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from datetime import datetime, timedelta
from openai import OpenAI

# ==========================================
# 🔐 CONFIGURATION
# ==========================================
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
OPENROUTER_API_KEY = os.environ.get("OPENROUTER_API_KEY") 
SENDER_EMAIL = os.environ.get("SENDER_EMAIL")
GMAIL_PASSWORD = os.environ.get("GMAIL_APP_PASSWORD")
RECIPIENT_EMAIL = "reezaalarafat@gmail.com" # Change if needed

# AI Client
client = OpenAI(
    base_url="https://openrouter.ai/api/v1",
    api_key=OPENROUTER_API_KEY,
)

def fetch_monthly_data():
    print("📡 Fetching Monthly Data from Supabase...")
    headers = {"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}"}
    
    all_data = []
    offset = 0
    batch_size = 1000
    
    while True:
        url = f"{SUPABASE_URL}/rest/v1/fires?select=*&offset={offset}&limit={batch_size}"
        try:
            r = requests.get(url, headers=headers)
            data = r.json()
            if not data: break
            all_data.extend(data)
            offset += batch_size
            print(f"   Fetched {len(all_data)} rows...", end="\r")
        except Exception as e:
            print(f"❌ DB Error: {e}")
            break
            
    return pd.DataFrame(all_data) if all_data else pd.DataFrame()

def wipe_database():
    """
    CRITICAL: Deletes all data to reset for the new month.
    Only runs if email was successful.
    """
    print("\n🧹 STARTING DATABASE CLEANUP...")
    headers = {
        "apikey": SUPABASE_KEY, 
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Prefer": "return=minimal"
    }
    
    # DELETE condition: ID > 0 (Deletes everything)
    url = f"{SUPABASE_URL}/rest/v1/fires?id=gt.0"
    
    try:
        r = requests.delete(url, headers=headers)
        if r.status_code in [200, 204]:
            print("✅ Database Wiped Successfully. Ready for next month.")
        else:
            print(f"❌ Wipe Failed: {r.status_code} {r.text}")
    except Exception as e:
        print(f"❌ Wipe Error: {e}")

def apply_thesis_physics(df):
    """
    Adds the Thesis Calculations (CO2, PM2.5, Energy)
    """
    if df.empty: return df
    
    # Ensure float conversion
    df['frp_mw'] = pd.to_numeric(df['frp_mw'], errors='coerce').fillna(0.0)
    
    # 1. Energy Calculation (Joules)
    # Fire Radiative Energy (MJ) = MW * Duration (Assuming 3600s for persistence)
    df['energy_mj'] = df['frp_mw'] * 3600 
    
    # 2. Biomass Consumption (Seiler & Crutzen, 1980)
    # Biomass (kg) = FRE (MJ) * 0.41 kg/MJ
    df['biomass_tonnes'] = (df['energy_mj'] * 0.41) / 1000
    
    # 3. Emissions (Akagi et al., 2011 for Ag Residue)
    # CO2 = 1585 g/kg biomass
    # PM2.5 = 6.3 g/kg biomass
    df['co2_tonnes'] = df['biomass_tonnes'] * 1.585
    df['pm25_kg'] = df['biomass_tonnes'] * 6.3
    
    return df

def get_ai_report(stats):
    try:
        prompt = f"""
        ACT AS: Senior Atmospheric Scientist.
        CONTEXT: Monthly audit of stubble burning in India (SATARK Thesis Data).
        
        DATA:
        - Events: {stats['count']}
        - Max Intensity: {stats['max_mw']:.1f} MW
        - Est. Biomass Burnt: {stats['biomass']:.1f} Tonnes
        - CO2 Emitted: {stats['co2']:.1f} Tonnes
        - PM2.5 Load: {stats['pm25']:.1f} kg
        
        TASK: Write a 'Strategic Research Impact Report'.
        1. **Trend Analysis**: Interpret the severity.
        2. **Toxicology**: Impact of {stats['pm25']:.1f} kg PM2.5 on local populations.
        3. **Thesis Validation**: How this data proves the "Evening Fire" hypothesis (fires occurring > 4PM).
        
        KEEP IT STRICT, SCIENTIFIC, AND BRIEF (Markdown).
        """
        
        completion = client.chat.completions.create(
            model="meta-llama/llama-3.3-70b-instruct:free",
            messages=[{"role": "user", "content": prompt}]
        )
        return completion.choices[0].message.content
    except:
        return "AI Analysis Offline."

def send_email(report_path, map_path, raw_csv_path, thesis_csv_path):
    if not SENDER_EMAIL: return False
    
    msg = MIMEMultipart()
    msg['From'] = SENDER_EMAIL
    msg['To'] = RECIPIENT_EMAIL
    msg['Subject'] = f"📑 SATARK MONTHLY DATA: {datetime.now().strftime('%B %Y')}"
    
    body = """
    ATTACHED:
    1. 📄 RAW_DB_DATA.csv (Direct from Supabase - Source of Truth)
    2. 🧪 THESIS_PHYSICS.csv (Processed with Emission Calculations)
    3. 🗺️ Intensity Map (.png)
    4. 🤖 AI Impact Report (.md)
    
    NOTE: Database will be WIPED after this email is delivered to reset storage.
    """
    msg.attach(MIMEText(body, 'plain'))
    
    # Attachments
    files_to_attach = [report_path, map_path, raw_csv_path, thesis_csv_path]
    
    for fpath in files_to_attach:
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
        print("✅ Email Sent Successfully.")
        return True
    except Exception as e:
        print(f"❌ Email Failed: {e}")
        return False

def run_audit():
    print(f"🚀 SATARK AUDIT PROTOCOL | {datetime.now().strftime('%Y-%m')}")
    os.makedirs('audit_out', exist_ok=True)
    
    # 1. FETCH RAW DATA
    df = fetch_monthly_data()
    if df.empty:
        print("⚠️ Database empty. Nothing to report.")
        return

    # 2. SAVE RAW CSV (Dataset A - The Evidence)
    raw_csv_file = f"audit_out/SATARK_RAW_DB_{datetime.now().strftime('%Y-%m')}.csv"
    df.to_csv(raw_csv_file, index=False)
    print(f"💾 Raw DB Data Saved ({len(df)} rows)")

    # 3. APPLY PHYSICS & SAVE THESIS CSV (Dataset B - The Science)
    df_physics = apply_thesis_physics(df.copy())
    thesis_csv_file = f"audit_out/SATARK_THESIS_PHYSICS_{datetime.now().strftime('%Y-%m')}.csv"
    df_physics.to_csv(thesis_csv_file, index=False)
    print(f"🧪 Physics Calculations Applied & Saved")
    
    # 4. MAP GENERATION (Using Physics Data)
    plt.figure(figsize=(10, 8), facecolor='#121212')
    ax = plt.axes(); ax.set_facecolor("#121212")
    sc = plt.scatter(df_physics['lon'], df_physics['lat'], c=df_physics['frp_mw'], cmap='plasma', s=40, alpha=0.9)
    plt.colorbar(sc, label='Fire Intensity (MW)')
    plt.title(f"FIRE DENSITY | {datetime.now().strftime('%B %Y')}", color='white')
    
    map_file = "audit_out/intensity_map.png"
    plt.savefig(map_file, dpi=150, bbox_inches='tight')
    plt.close()

    # 5. AI REPORT
    stats = {
        'count': len(df_physics),
        'max_mw': df_physics['frp_mw'].max(),
        'biomass': df_physics['biomass_tonnes'].sum(),
        'co2': df_physics['co2_tonnes'].sum(),
        'pm25': df_physics['pm25_kg'].sum()
    }
    ai_text = get_ai_report(stats)
    
    report_file = "audit_out/report.md"
    with open(report_file, "w") as f:
        f.write(f"# SATARK MONTHLY DATA\n\n{ai_text}")

    # 6. SEND & WIPE
    success = send_email(report_file, map_file, raw_csv_file, thesis_csv_file)
    
    if success:
        print("🔒 Security Protocol: Wiping Cloud Database...")
        wipe_database()
        print("👋 System Reset Complete. See you next month.")
    else:
        print("⚠️ Email Failed. Database preserved for safety.")

if __name__ == "__main__":
    run_audit()
