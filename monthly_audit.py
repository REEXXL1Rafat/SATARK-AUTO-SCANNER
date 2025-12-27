import os
import pandas as pd
import matplotlib.pyplot as plt
import google.generativeai as genai
from datetime import datetime
import glob

# 1. INITIALIZE COGNITIVE ENGINE
genai.configure(api_key=os.environ.get("GEMINI_API_KEY"))
model = genai.GenerativeModel('gemini-2.0-flash')

def run_monthly_audit():
    print("🚀 SATARK MONTHLY AUDIT ENGINE: STARTING...")
    
    # 2. DATA AGGREGATION (The "File Gobbler")
    report_dir = 'weekly_reports'
    files = glob.glob(f"{report_dir}/*.csv")
    
    if not files:
        print("⚠️ No weekly reports found. Check directory structure.")
        return

    # Merge and Deduplicate by Supabase ID
    master_df = pd.concat([pd.read_csv(f) for f in files]).drop_duplicates(subset=['id'])
    print(f"📈 Total Unique Events for Audit: {len(master_df)}")

    # 3. ECONOMIC IMPACT MODELING (EIAM)
    master_df['ha'] = master_df['est_area_m2'] / 10000
    
    def calculate_cost(row):
        # Zone A (Agri/Stubble) = ₹3.7 Lakh/Ha | Others/Forest = ₹58k/Ha
        if row['research_zone'] == "ZONE_A_NORTH":
            return row['ha'] * 370000 
        else:
            return row['ha'] * 58000
            
    master_df['loss_inr'] = master_df.apply(calculate_cost, axis=1)

    # 4. GENERATE VISUAL THREAT MAP (Heatmap)
    plt.figure(figsize=(10, 8), facecolor='#121212')
    ax = plt.axes()
    ax.set_facecolor("#121212")
    
    colors = master_df['research_zone'].map({
        'ZONE_A_NORTH': '#FF3131', # Red
        'ZONE_D_CENTRAL': '#FFBD03', # Orange
        'ZONE_B_EAST': '#00E5FF', # Cyan
        'ZONE_C_SOUTH': '#70FF00' # Green
    }).fillna('#FFFFFF')

    plt.scatter(master_df['lon'], master_df['lat'], s=master_df['ha']*3, c=colors, alpha=0.7)
    plt.title(f"SATARK MONTHLY THREAT MAP | {datetime.now().strftime('%B %Y')}", color='white', pad=20)
    plt.grid(color='#333333', linestyle='--', alpha=0.5)
    
    os.makedirs('monthly_audits', exist_ok=True)
    map_filename = f"monthly_audits/MAP_{datetime.now().strftime('%Y-%m')}.png"
    plt.savefig(map_filename, dpi=300, bbox_inches='tight')
    plt.close()

    # 5. GENERATE STRATEGIC MEMO (Gemini 2.0)
    total_loss_cr = master_df['loss_inr'].sum() / 10000000
    total_ha = master_df['ha'].sum()
    zone_counts = master_df.groupby('research_zone').size().to_dict()

    prompt = f"""
    ROLE: Senior Environmental Security Advisor.
    PLATFORM: SATARK (Autonomous Geospatial Defense).
    DATA:
    - Month: {datetime.now().strftime('%B %Y')}
    - Unique Fire Detections: {len(master_df)}
    - Total Area Impacted: {total_ha:.2f} Hectares
    - Estimated Total Social Cost (EIAM): ₹{total_loss_cr:.2f} Crores
    - Geographic Clusters: {zone_counts}

    TASK: Generate a high-level Strategic Memo. 
    1. Summarize the 'Fiscal Hemorrhage' caused by these events. 
    2. Identify the highest-risk zone based on the data. 
    3. Provide three 'No-Nonsense' policy interventions. 
    Tone: Professional, urgent, data-driven.
    """

    print("🤖 Gemini is synthesizing the Strategic Brief...")
    response = model.generate_content(prompt)
    
    # 6. ARCHIVE FINAL OUTPUTS
    report_filename = f"monthly_audits/STRATEGIC_REPORT_{datetime.now().strftime('%Y-%m')}.md"
    with open(report_filename, "w") as f:
        f.write(f"# SATARK MONTHLY STRATEGIC BRIEF // {datetime.now().strftime('%B %Y')}\n\n")
        f.write(f"![Threat Map]({os.path.basename(map_filename)})\n\n")
        f.write(response.text)
    
    master_df.to_csv(f"monthly_audits/DATA_{datetime.now().strftime('%Y-%m')}.csv", index=False)
    print(f"✅ Audit Complete. Files saved to /monthly_audits")

if __name__ == "__main__":
    run_monthly_audit()
