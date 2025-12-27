import os
import pandas as pd
import matplotlib.pyplot as plt
from google import genai  # NEW SDK IMPORT
from datetime import datetime
import glob

# 1. INITIALIZE MODERN CLIENT
# It automatically looks for GEMINI_API_KEY in your GitHub Secrets environment
client = genai.Client(api_key=os.environ.get("GEMINI_API_KEY"))

def run_monthly_audit():
    print("🚀 SATARK MONTHLY AUDIT ENGINE (V7.0): STARTING...")
    
    # 2. DATA AGGREGATION
    report_dir = 'weekly_reports'
    files = glob.glob(f"{report_dir}/*.csv")
    if not files:
        print("⚠️ No weekly reports found. Check if /weekly_reports folder exists.")
        return

    # Merge all weekly reports and remove duplicates based on 'id'
    master_df = pd.concat([pd.read_csv(f) for f in files]).drop_duplicates(subset=['id'])
    print(f"📈 Total Unique Events for Audit: {len(master_df)}")

    # 3. ECONOMIC IMPACT CALCULATION (EIAM)
    master_df['ha'] = master_df['est_area_m2'] / 10000
    
    def calculate_cost(row):
        # Zone A (Agri/Stubble) = ₹3.7 Lakh/Ha | Forest/Other = ₹58k/Ha
        if row['research_zone'] == "ZONE_A_NORTH":
            return row['ha'] * 370000 
        else:
            return row['ha'] * 58000
            
    master_df['loss_inr'] = master_df.apply(calculate_cost, axis=1)

    # 4. GENERATE VISUAL THREAT MAP
    plt.figure(figsize=(10, 8), facecolor='#121212')
    ax = plt.axes()
    ax.set_facecolor("#121212")
    
    colors = master_df['research_zone'].map({
        'ZONE_A_NORTH': '#FF3131', 
        'ZONE_D_CENTRAL': '#FFBD03',
        'ZONE_B_EAST': '#00E5FF',
        'ZONE_C_SOUTH': '#70FF00'
    }).fillna('#FFFFFF')

    plt.scatter(master_df['lon'], master_df['lat'], s=master_df['ha']*3, c=colors, alpha=0.7)
    plt.title(f"SATARK THREAT MAP | {datetime.now().strftime('%B %Y')}", color='white', pad=20)
    plt.grid(color='#333333', linestyle='--', alpha=0.5)
    
    os.makedirs('monthly_audits', exist_ok=True)
    map_filename = f"monthly_audits/MAP_{datetime.now().strftime('%Y-%m')}.png"
    plt.savefig(map_filename, dpi=300, bbox_inches='tight')
    plt.close()

    # 5. COGNITIVE SYNTHESIS (NEW SDK CALL)
    total_loss_cr = master_df['loss_inr'].sum() / 10000000
    total_ha = master_df['ha'].sum()
    zone_counts = master_df.groupby('research_zone').size().to_dict()

    prompt = f"""
    ROLE: Senior Environmental Security Advisor.
    PLATFORM: SATARK (Autonomous Geospatial Defense).
    DATA SUMMARY:
    - Month: {datetime.now().strftime('%B %Y')}
    - Unique Fire Detections: {len(master_df)}
    - Total Area Impacted: {total_ha:.2f} Hectares
    - Estimated Total Social Cost: ₹{total_loss_cr:.2f} Crores
    - Zone Breakdown: {zone_counts}

    TASK: Generate a professional Strategic Memo. 
    Focus on the financial damage and identify the highest-risk zone. 
    Provide three urgent policy interventions. Tone: Serious and data-driven.
    """

    print("🤖 Gemini 2.0 is analyzing the month's data...")
    try:
        # Modern generate_content call
        response = client.models.generate_content(
            model='gemini-2.0-flash', 
            contents=prompt
        )
        report_text = response.text
    except Exception as e:
        report_text = f"ANALYSIS ERROR: {e}\n(System Note: Verify Gemini API billing and quota status in Google AI Studio.)"

    # 6. ARCHIVE OUTPUTS
    report_filename = f"monthly_audits/STRATEGIC_REPORT_{datetime.now().strftime('%Y-%m')}.md"
    with open(report_filename, "w") as f:
        f.write(f"# SATARK MONTHLY STRATEGIC BRIEF // {datetime.now().strftime('%B %Y')}\n\n")
        f.write(f"![Threat Map]({os.path.basename(map_filename)})\n\n")
        f.write(report_text)
    
    master_df.to_csv(f"monthly_audits/DATA_{datetime.now().strftime('%Y-%m')}.csv", index=False)
    print(f"✅ Audit Complete. Strategic Report generated at {report_filename}")

if __name__ == "__main__":
    run_monthly_audit()
