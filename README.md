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


