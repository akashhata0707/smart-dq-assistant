# Architecture Decision Records (ADR)

This document explains the key technical decisions made in this project and the reasoning behind each choice.

---

## ADR-001: PySpark over Pandas

**Decision:** Use PySpark for data processing instead of Pandas.

**Reasoning:**
- Dataset size is 1M rows — Pandas loads everything into memory, PySpark processes in parallel
- Mirrors real production environments where data is 10GB-10TB
- Demonstrates enterprise-grade data engineering skills relevant to job roles

**Tradeoff:** More complex setup, slower for small datasets.

---

## ADR-002: Delta Lake over Parquet

**Decision:** Use Delta Lake as the storage format for all pipeline layers.

**Reasoning:**
- ACID transactions — safe concurrent reads/writes
- Time travel — ability to query previous versions of data
- Schema enforcement — prevents bad schema changes from breaking pipelines
- Native support for MERGE (upserts) needed for SCD patterns

**Tradeoff:** Requires Delta Lake dependency, slightly more storage overhead.

---

## ADR-003: Custom DQ Engine over Great Expectations

**Decision:** Build a custom DQ engine instead of using Great Expectations.

**Reasoning:**
- Great Expectations has known compatibility issues with Python 3.12
- Custom engine gives full control over check types and output format
- Easier to integrate with LLM layer — GE's output format is complex to parse
- Simpler to explain in interviews — every line of code is owned by us

**Tradeoff:** Less feature-rich than GE out of the box.

---

## ADR-004: Groq (Llama 3.3) over OpenAI/Anthropic

**Decision:** Use Groq API with Llama 3.3 70B model.

**Reasoning:**
- Free tier available — no cost during development
- Extremely fast inference (Groq hardware is optimized for speed)
- Llama 3.3 70B is capable enough for structured JSON generation
- Works reliably in India without regional restrictions

**Tradeoff:** Less capable than GPT-4 or Claude for complex reasoning tasks.

---

## ADR-005: Pydantic for LLM Output Validation

**Decision:** Use Pydantic models to validate all LLM outputs.

**Reasoning:**
- LLMs occasionally return malformed JSON — Pydantic catches this immediately
- Type safety — confidence scores are always floats, severity is always a string
- Prevents pipeline crashes from bad LLM responses
- Shows senior engineering thinking — never trust raw LLM output

**Tradeoff:** Requires defining models upfront, adds a small overhead.

---

## ADR-006: Streamlit over Flask/FastAPI for Dashboard

**Decision:** Use Streamlit for the dashboard instead of a full web framework.

**Reasoning:**
- Built for data apps — charts, tables, and widgets out of the box
- Zero frontend code needed — pure Python
- Fast to iterate — changes reflect immediately
- Easy to deploy on Streamlit Cloud for free

**Tradeoff:** Less customizable than a full React/Flask app.

---

## ADR-007: Medallion Architecture (Bronze/Silver/Gold)

**Decision:** Implement three-layer Medallion Architecture.

**Reasoning:**
- Industry standard pattern used at Databricks, Azure, and most enterprise data platforms
- Clear separation of concerns — raw data, cleaned data, aggregated data
- DQ checks run on Silver (not Bronze) — Bronze is intentionally raw
- Matches real production experience from UnitedHealth and ReNew Power projects

**Tradeoff:** More storage used, more pipeline complexity.

---

## ADR-008: Spark Structured Streaming over Kafka

**Decision:** Use Spark Structured Streaming with file-based micro-batches instead of Kafka.

**Reasoning:**
- Kafka requires additional infrastructure (broker, zookeeper) — adds complexity
- File-based streaming is simpler to run locally and demonstrate
- `foreachBatch` pattern works identically whether source is Kafka or files
- Demonstrates streaming concepts without operational overhead

**Tradeoff:** Not truly real-time — micro-batch latency of 10-30 seconds.