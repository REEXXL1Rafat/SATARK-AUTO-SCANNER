# SATARK: Autonomous Hyper-Local Climate Intelligence Network

[![Status](https://img.shields.io/badge/Status-Field%20Trials%20(West%20Bengal)-success)]()
[![Engine](https://img.shields.io/badge/Core-SATARK%20v2.0-blue)]()
[![Research](https://img.shields.io/badge/Method-Mixed%20Methods-orange)]()
[![License](https://img.shields.io/badge/License-MIT-green)]()

> **"You cannot arrest a million farmers for doing the math of survival. A ban is just a piece of paper against a wall of fire. If the economics force them to burn, we don't need police—we need a better equation."**

## 📖 The Research Thesis
This repository houses the computational engine for a multidimensional socio-economic study on the **Stubble Burning Crisis** in  India. 

Current policy treats agricultural fire as a criminal act. My research argues it is an **economic inevitability**. By combining high-frequency satellite telemetry with ground-level anthropological interviews, this project aims to quantify the "Time-Cost Squeeze" that forces farmers to burn, while measuring the exact ecological debt incurred per acre.

**The Dimensions:**
1.  **The Eye (Quantitative):** Real-time detection of "micro-fires" often missed by global datasets.
2.  **The Voice (Qualitative):** Interviews with small-plot farmers to map the psychology of "The Burn."
3.  **The Cost (Ecological):** Algorithmic conversion of thermal intensity (MW) into PM2.5 and CO2 output.

---

## 🚀 The SATARK Engine

SATARK (Sanskrit for *Alert*) is the technical backbone of this research. It is an autonomous "Hybrid Intelligence" network designed to validate satellite anomalies with semantic ground truth.

### Core Architecture
The system operates on a **Detect-Verify-Quantify** loop, bypassing the 4-6 hour latency of standard government reports.

```mermaid
graph TD
    A[NASA VIIRS/MODIS] -->|Raw Thermal Data| B(SATARK Engine)
    B -->|Filter Glint/Industrial| C{Llama 3.3 Guard}
    C -->|Context: Mines vs Farms| D[Semantic Verification]
    D -->|Valid Fire| E[Physics Kernel]
    E -->|Calc: Biomass & Toxicity| F[Supabase DB]
    F -->|Alert BDO/Admin| G[Real-Time Intervention]
    F -->|Monthly Audit| H[Ecological Damage Report]


>
Key Technical Innovations1. The Llama 3 Border GuardStandard algorithms confuse steel mills for farm fires. SATARK implements an LLM-based filter (satark_cloud_v5.py):Logic: Uses meta-llama-3-70b-instruct to analyze geolocation context against OpenStreetMap tags.Function: Distinguishes "Industrial Heat" (Mines, Quarries) from "Biomass Combustion" (Farms) to reduce false positives.2. The Physics KernelWe don't just count fires; we measure consequence. The monthly_audit.py module implements:Wooster et al. (2005) Radiance Conversion: Converts Fire Radiative Power (MW) into Megajoules of energy.Emission Factors: Calculates precise tonnage of PM2.5 (Aerosol) and CO2 injected into the local atmosphere.📂 Codebase ManifestFileDescriptionsatark_cloud_v5.pyThe Scanner. Fetches NASA VIIRS data, runs the Llama 3 semantic check, and logs verified fires to Supabase.monthly_audit.pyThe Auditor. Generates a "Strategic Ecological Impact Report" using Llama 3.3, analyzing the monthly carbon footprint.weekly_intel.pyThe Aggregator. Compiles weekly fire trends for local block administration.requirements.txtCore dependencies: pandas, rasterio, openai, google-generativeai.📉 Preliminary Observations (Pilot Phase)The Latency Gap: SATARK detects fires ~4 hours faster than the local forest department communication chain.The "Micro-Fire" Phenomenon: 30% of fires detected in West Bengal are <50MW, often filtered out by global "high-confidence" masks but cumulatively toxic.The Human Factor: Early interviews indicate burning is correlated inversely with the availability of affordable labor, not awareness of pollution.⚡ Quick Start (Replication)Bash# Clone the repository
git clone [https://github.com/reexxl1rafat/satark-research.git](https://github.com/reexxl1rafat/satark-research.git)

# Install dependencies
pip install -r requirements.txt

# Set Environment Variables (NASA FIRMS + OpenAI/OpenRouter)
export NASA_KEY="your_key"
export OPENROUTER_API_KEY="your_key"

# Run a Sector Scan
python satark_cloud_v5.py
📜 CitationIf you utilize the code, dataset, or methodology, please cite the ongoing research:Sk Reezaal Arafat (2025).The Combustion Paradox: Deconstructing the Socio-Economic Drivers and Multidimensional Ecological Flux of India’s Stubble Crisis [Pre-print/Field Notes].Built with rage and hope in the paddy fields of West Bengal.
