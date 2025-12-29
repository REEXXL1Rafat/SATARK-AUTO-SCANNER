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



Key Technical Innovations
1) Llama 3.3 Border Guard (Semantic Verification)


Purpose: Remove non-agricultural heat sources (steel mills, mines, industrial flares) from fire alerts.


How it works: Uses meta-llama-3-70b-instruct to cross-reference event coordinates with OpenStreetMap land-use tags and contextual signals (temporal patterns, nearby infrastructure).


Outcome: High precision classification into Survival Combustion vs Industrial Heat and drastically fewer false flags.


2) Physics Kernel (Emission Auditing)


Radiance → Energy: Converts Fire Radiative Power to megajoules using Wooster et al. (2005) coefficients.


Emission Modeling: Converts measured energy to PM2.5 and CO₂ tonnage per event using empirically derived emission factors and biomass models.


Reports: Produces auditable per-event emission records for aggregation and policy use.



Codebase Manifest
FileDescriptionsatark_cloud_v5.pyThe Scanner. Fetches NASA telemetry, runs Llama 3 semantic checks, manages Supabase DB, emits alerts.monthly_audit.pyThe Auditor. Monthly strategic ecological impact reports and carbon footprint accounting.weekly_intel.pyThe Aggregator. High-level trend analysis, temporal clustering, and sector summaries for administrators.requirements.txtCore dependencies (pandas, rasterio, openai, supabase-py, etc.).

Preliminary Performance (Pilot Phase — West Bengal)


Latency Gap: SATARK typically detects and verifies events ~4 hours earlier than the local forest department communications chain.


Micro-Fire Detection: ~30% of verified fires were <50 MW — events that national reporting frequently misses.


Human Factor: Burning frequency is inversely correlated with local labor availability — indicating economic drivers rather than lack of awareness.



Quick Start (Replication)
# Clone the SATARK Engine
git clone https://github.com/reexxl1rafat/satark-auto-scanner.git
cd satark-auto-scanner

# Install Dependencies
pip install -r requirements.txt

# Set Environment Variables
export NASA_KEY="your_key"
export OPENROUTER_API_KEY="your_key"
export SUPABASE_URL="https://your-supabase-url"
export SUPABASE_KEY="your-supabase-key"

# Run a Sector Scan (example)
python satark_cloud_v5.py --bbox "88.3,23.0,88.5,23.3" --start "2025-01-01T00:00:00Z" --end "2025-01-01T06:00:00Z"

Notes


The engine expects FIRMS/VIIRS and MODIS feeds. If you prefer, swap to other telemetry sources but ensure radiance scaling is matched.


satark_cloud_v5.py has flags for scan resolution, verification mode, and dry-run.



Configuration & Environment
Minimum required environment variables:


NASA_KEY — NASA FIRMS / telemetry access key.


OPENROUTER_API_KEY — API key for OpenRouter or preferred LLM endpoint.


SUPABASE_URL and SUPABASE_KEY — Supabase instance for storing verified events and audit logs.


Optional configuration:


Region bounding boxes, alert webhooks, and OSM augmentation cache path can be set via config.yaml or environment flags.



Data Model & Auditing


Each verified event includes:


event_id, datetime, latitude, longitude


raw_frp (MW), radiance_converted_energy (MJ)


classification (farm | industrial | ambiguous)


pm25_tonnage, co2_tonnage


verification_passes (LLM score + geospatial checks)


audit_hash (cryptographic hash for tamper evidence)




Data is stored in Supabase and exposed via REST and CSV export endpoints for integration with dashboards and sheets.



Exporting to Sheets & Downstream Use


The engine includes CSV and JSON exporters. For Google Sheets integration, use any of:


gspread + service account script to push CSV rows to a sheet.


Supabase → Google Data Studio / Looker Studio connector for dashboards.




monthly_audit.py outputs both human-readable PDF summaries and machine-readable CSVs for policy teams.



Research & Citation
Data generated by SATARK supports:
Arafat, S. R. (2025). The Combustion Paradox: Deconstructing the Socio-Economic Drivers of India’s Stubble Crisis.
If you reuse SATARK data in research or policy materials, cite the dataset and include the repository commit hash used for reproducibility.

Operational Considerations


Ground Truth: Build partnerships with local administrations and NGOs for on-the-ground verifications to continuously retrain the semantic guard.


Ethics & Privacy: Do not expose precise farmer identities or private property details. Use event aggregation windows and access controls for sensitive outputs.


Governance: Maintain an audit trail (cryptographic hashing) for all alerts and reports to ensure defensible evidence for intervention decisions.



Contributing
Contributions should prioritize:


Improved false-positive reduction for industrial signatures.


More accurate emission factors for regional crop types.


Robustness to telemetry noise and sensor outages.


Create PRs against main, include tests for ingest → verify → quantify flow, and tag large model changes in the changelog.

License
Suggested: MIT License — include LICENSE when ready. Ensure any third-party model or dataset licensing terms (e.g., LLM provider, NASA data terms) are respected.

Contact & Operational Onboarding
For operational deployment, auditing questions, or replication support, open an issue in the repo with:


Description of your region of interest


Desired scan cadence


Available telemetry sources and keys





