import os
import pandas as pd
import matplotlib.pyplot as plt
from openai import OpenAI # We use this to talk to OpenRouter
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from datetime import datetime
import glob
import json

# 1. SETUP OPENROUTER (The "Free Access" Gateway)
client = OpenAI(
    base_url="https://openrouter.ai/api/v1",
    api_key=os.environ.get("OPENROUTER_API_KEY"),
)

# Email Secrets
RECIPIENT_EMAIL = "reezaalarafat@gmail.com"
SENDER_EMAIL = os.environ.get("SENDER_EMAIL")
GMAIL_PASSWORD = os.environ.get("GMAIL_APP_PASSWORD")

def send_intel_email(report_path, map_path):
    if not SENDER_EMAIL or not GMAIL_PASSWORD: return
    print("📧 Sending Dispatch...")
    msg = MIMEMultipart()
    msg['From'] = SENDER_EMAIL
    msg['To'] = RECIPIENT_EMAIL
    msg['Subject'] = f"🚨 SATARK DEEP INTEL: {datetime.now().strftime('%B %Y')}"
    msg.attach(MIMEText("Attached: Multi-Dimensional Strategic Audit.", 'plain'))

    for path in [report_path, map_path]:
        if os.path.exists(path):
            with open(path, "rb") as f:
                part = MIMEBase("application", "octet-stream")
                part.set_payload(f.read())
                encoders.encode_base64(part)
                part.add_header("Content-Disposition", f"attachment; filename={os.path.basename(path)}")
                msg.attach(part)

    with smtplib.SMTP("smtp.gmail.com", 587) as server:
        server.starttls()
        server.login(SENDER_EMAIL, GMAIL_PASSWORD)
        server.send_message(msg)
    print("✅ Email Sent.")

def run_monthly_audit():
    print("🚀 SATARK V9.0 (OpenRouter Edition): STARTING...")
    
    # 2. DATA AGGREGATION
    files = glob.glob("weekly_reports/*.csv")
    if not files:
        print("⚠️ No data found.")
        return

    master_df = pd.concat([pd.read_csv(f) for f in files]).drop_duplicates(subset=['id'])
    
    # 3. MULTI-DIMENSIONAL CALCULATIONS
    master_df['ha'] = master_df['est_area_m2'] / 10000
    # Economic Loss (INR)
    master_df['loss_inr'] = master_df.apply(lambda x: x['ha']*370000 if x['research_zone']=="ZONE_A_NORTH" else x['ha']*58000, axis=1)
    # CO2 Estimate (Approx 20 tonnes per hectare for crop fires)
    master_df['co2_tonnes'] = master_df['ha'] * 20 
    
    # 4. MAP GENERATION
    plt.figure(figsize=(10, 8), facecolor='#121212')
    ax = plt.axes(); ax.set_facecolor("#121212")
    colors = master_df['research_zone'].map({'ZONE_A_NORTH':'#FF3131','ZONE_D_CENTRAL':'#FFBD03','ZONE_B_EAST':'#00E5FF','ZONE_C_SOUTH':'#70FF00'}).fillna('#FFFFFF')
    plt.scatter(master_df['lon'], master_df['lat'], s=master_df['ha']*5, c=colors, alpha=0.7)
    plt.title(f"SATARK THREAT MAP | {datetime.now().strftime('%B %Y')}", color='white')
    os.makedirs('monthly_audits', exist_ok=True)
    map_filename = f"monthly_audits/MAP_{datetime.now().strftime('%Y-%m')}.png"
    plt.savefig(map_filename, dpi=300, bbox_inches='tight'); plt.close()

    # 5. DEEP ANALYSIS (Via OpenRouter)
    total_loss_cr = master_df['loss_inr'].sum() / 10000000
    total_co2 = master_df['co2_tonnes'].sum()
    zone_breakdown = master_df.groupby('research_zone').size().to_dict()

    prompt = f"""
    ACT AS: Chief Strategy Officer for Climate Defense.
    
    INPUT DATA:
    - Month: {datetime.now().strftime('%B %Y')}
    - Total Fires: {len(master_df)}
    - Economic Loss: ₹{total_loss_cr:.2f} Crores
    - Ecological Load: {total_co2:.1f} Tonnes of CO2
    - Zone Breakdown: {zone_breakdown}

    TASK: Write a Multi-Dimensional Strategic Report.
    
    DIMENSION 1: FINANCIAL IMPACT
    - Analyze the ₹{total_loss_cr:.2f} Cr loss. Is this sustainable? 
    
    DIMENSION 2: PUBLIC HEALTH & ECOLOGY
    - Discuss the impact of {total_co2:.1f} Tonnes of CO2 on local populations (AQI impact).
    
    DIMENSION 3: STRATEGIC INTERVENTION
    - Recommend 2 specific, high-tech interventions for the worst-hit zone.

    OUTPUT FORMAT: Professional Markdown. Use Headers. No fluff.
    """

    print("🤖 OpenRouter (Gemini/Llama) is analyzing...")
    try:
        # We ask for the 'free' Gemini 2.0 Flash model
        # If unavailable, you can swap string to 'meta-llama/llama-3.3-70b-instruct:free'
        completion = client.chat.completions.create(
            model="google/gemini-2.0-flash-exp:free", 
            messages=[{"role": "user", "content": prompt}]
        )
        report_text = completion.choices[0].message.content
    except Exception as e:
        report_text = f"ANALYSIS FAILED: {e}"

    # 6. ARCHIVE & SEND
    report_filename = f"monthly_audits/REPORT_{datetime.now().strftime('%Y-%m')}.md"
    with open(report_filename, "w") as f:
        f.write(f"# SATARK DEEP INTEL // {datetime.now().strftime('%B %Y')}\n\n![Map]({os.path.basename(map_filename)})\n\n{report_text}")
    
    master_df.to_csv(f"monthly_audits/DATA_{datetime.now().strftime('%Y-%m')}.csv", index=False)
    send_intel_email(report_filename, map_filename)
    print("✅ Audit Complete.")

if __name__ == "__main__":
    run_monthly_audit()
