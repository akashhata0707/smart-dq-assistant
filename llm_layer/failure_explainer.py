import json
from groq import Groq
from pydantic import BaseModel
import os
from dotenv import load_dotenv

load_dotenv()
client = Groq(api_key=os.getenv("GROQ_API_KEY"))


class RootCauseAnalysis(BaseModel):
    failed_check: str
    column: str
    explanation: str
    confidence: float
    recommended_investigation: str
    severity: str


def explain_failure(failed_check, column, current_stats, previous_stats) -> RootCauseAnalysis:
    prompt = f"""You are a senior data engineer performing root cause analysis.

FAILED CHECK: {failed_check}
COLUMN: {column}
PREVIOUS STATS: {json.dumps(previous_stats)}
CURRENT STATS: {json.dumps(current_stats)}

Return ONLY JSON:
{{
  "failed_check": "{failed_check}",
  "column": "{column}",
  "explanation": "detailed explanation",
  "confidence": 0.85,
  "recommended_investigation": "specific steps",
  "severity": "HIGH"
}}"""

    response = client.chat.completions.create(
        model="llama-3.3-70b-versatile",
        messages=[{"role": "user", "content": prompt}],
        temperature=0.1
    )
    raw = response.choices[0].message.content.strip()
    raw = raw.replace("```json", "").replace("```", "").strip()
    return RootCauseAnalysis.model_validate_json(raw)