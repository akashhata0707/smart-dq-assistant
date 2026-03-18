# 🔍 Smart Data Quality Assistant

> AI-powered data quality monitoring for e-commerce pipelines using PySpark, Delta Lake, and LLM.

---

## 🚀 What This Project Does

Most data pipelines fail silently — bad data flows through undetected until it breaks a dashboard or corrupts a report.

This project builds an intelligent data quality layer that:
- **Automatically generates** validation rules from data profiles using LLM
- **Detects failures** in real-time across streaming batches
- **Explains root causes** in plain English with confidence scores
- **Visualizes** DQ trends and failures in an interactive dashboard

---

## 🏗️ Architecture
```
Raw CSV (1M rows)
      ↓
Spark Structured Streaming
      ↓
Bronze Delta Table (raw ingestion)
      ↓
Silver Transformation (filtered + cleaned)
      ↓
Custom DQ Engine (9 check types)
      ↓
LLM Rule Generator ──── LLM Root Cause Explainer
      ↓                          ↓
  GE Rules JSON          Explanation + Confidence Score
      ↓
Streamlit Dashboard
```

---

## 📊 Key Metrics

| Metric | Value |
|---|---|
| Dataset size | 1,000,000 rows |
| Bad data injected | ~28% (5 failure types) |
| Silver filter rate | 28.61% rejected |
| DQ checks per batch | 9 checks |
| LLM rule generation | 10 rules auto-generated |
| Streaming batches | 5 micro-batches (200k rows each) |
| Root cause confidence | 0.85 avg |

---

## 🛠️ Tech Stack

| Layer | Technology |
|---|---|
| Data Processing | PySpark 3.5.0 |
| Storage | Delta Lake 3.1.0 |
| Orchestration | Spark Structured Streaming |
| Data Quality | Custom DQ Engine |
| LLM | Groq (Llama 3.3 70B) |
| Validation | Pydantic v2 |
| Dashboard | Streamlit + Plotly |
| Version Control | Git + GitHub |

---

## 📁 Project Structure
```
smart-dq-assistant/
│
├── data/
│   └── generate_data.py        # 1M row synthetic dataset generator
│
├── pipelines/
│   └── medallion_pipeline.py   # Bronze → Silver → Gold pipeline
│
├── dq_engine/
│   └── dq_engine.py            # Custom DQ Engine (9 check types)
│
├── llm_layer/
│   ├── rule_generator.py       # LLM auto rule generation
│   └── failure_explainer.py    # LLM root cause analysis
│
├── dashboard/
│   └── streamlit_app.py        # Streamlit UI
│
├── tests/
│   └── test_dq_engine.py       # Unit tests
│
├── DECISIONS.md                # Architecture decision records
└── README.md
```

---

## ⚙️ Setup & Run

### Prerequisites
- Python 3.12
- Java 11
- Groq API key (free at console.groq.com)

### Installation
```bash
git clone https://github.com/akashhata0707/smart-dq-assistant.git
cd smart-dq-assistant
python -m venv venv
venv\Scripts\activate
pip install pyspark==3.5.0 delta-spark==3.1.0 groq pydantic streamlit plotly python-dotenv
```

### Environment Setup
```bash
# Create .env file
echo GROQ_API_KEY=your_key_here > .env
```

### Generate Dataset
```bash
python data/generate_data.py
```

### Run Dashboard
```bash
streamlit run dashboard/streamlit_app.py
```

---

## 🎯 Key Features

### 1. Automatic Rule Generation
LLM reads data profile (null rates, distributions, top values) and suggests GE-compatible validation rules — no manual rule writing needed.

### 2. Root Cause Analysis
When a DQ check fails, the LLM analyzes current vs previous batch statistics and generates a plain-English explanation with confidence score and investigation steps.

### 3. Streaming Integration
Pipeline runs on Spark Structured Streaming with `foreachBatch` — DQ checks and LLM analysis trigger automatically on each micro-batch.

### 4. Medallion Architecture
Full Bronze → Silver → Gold implementation on Delta Lake with schema enforcement and incremental processing.

---

## 📈 Sample Output

### LLM Root Cause Analysis
```
Check    : expect_not_null
Column   : payment_method
Severity : HIGH
Confidence: 85%

Explanation:
The null rate increased from 1% → 34% between batches.
Likely cause: payment service API failed during ingestion
for orders created via mobile checkout on 2026-03-17.

Recommended Investigation:
Review ETL pipeline logs, check payment service API health,
verify ingestion job for 2026-03-17 partition.
```

---

## 🔗 Links
- **Demo Video:** [Coming soon]
- **Author:** Akash Hatagale
- **LinkedIn:** [your linkedin url]