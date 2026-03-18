import json
from groq import Groq
from pydantic import BaseModel
from typing import List
import os
from dotenv import load_dotenv

load_dotenv()
client = Groq(api_key=os.getenv("GROQ_API_KEY"))


class DQRule(BaseModel):
    column: str
    rule_type: str
    parameters: dict
    reasoning: str


class DQRuleSet(BaseModel):
    dataset_name: str
    rules: List[DQRule]
    summary: str


def generate_dq_rules(profile: dict, sample_rows: list) -> DQRuleSet:
    prompt = f"""You are a senior data quality engineer.

Given this data profile and sample rows, suggest data quality rules.

DATA PROFILE:
{json.dumps(profile, indent=2)}

SAMPLE ROWS:
{json.dumps(sample_rows, indent=2)}

ALLOWED RULE TYPES ONLY:
- not_null, value_range, unique, accepted_values, null_rate

Return ONLY JSON:
{{
  "dataset_name": "silver_orders",
  "rules": [
    {{
      "column": "column_name",
      "rule_type": "rule_type",
      "parameters": {{}},
      "reasoning": "why this rule"
    }}
  ],
  "summary": "brief summary"
}}"""

    response = client.chat.completions.create(
        model="llama-3.3-70b-versatile",
        messages=[{"role": "user", "content": prompt}],
        temperature=0.1
    )
    raw = response.choices[0].message.content.strip()
    raw = raw.replace("```json", "").replace("```", "").strip()
    return DQRuleSet.model_validate_json(raw)