import pytest
from pydantic import BaseModel
from typing import List
import json


# --- Test Pydantic Models ---
class DQRule(BaseModel):
    column: str
    rule_type: str
    parameters: dict
    reasoning: str


class DQRuleSet(BaseModel):
    dataset_name: str
    rules: List[DQRule]
    summary: str


class RootCauseAnalysis(BaseModel):
    failed_check: str
    column: str
    explanation: str
    confidence: float
    recommended_investigation: str
    severity: str


# --- Tests ---

def test_dq_rule_valid():
    rule = DQRule(
        column="order_amount",
        rule_type="value_range",
        parameters={"min_value": 0, "max_value": 5000},
        reasoning="Order amounts must be positive"
    )
    assert rule.column == "order_amount"
    assert rule.rule_type == "value_range"


def test_dq_ruleset_valid():
    ruleset = DQRuleSet(
        dataset_name="silver_orders",
        rules=[
            DQRule(
                column="order_id",
                rule_type="unique",
                parameters={},
                reasoning="Order IDs must be unique"
            )
        ],
        summary="Dataset looks clean"
    )
    assert ruleset.dataset_name == "silver_orders"
    assert len(ruleset.rules) == 1


def test_root_cause_valid():
    analysis = RootCauseAnalysis(
        failed_check="expect_not_null",
        column="payment_method",
        explanation="Null rate increased from 1% to 34%",
        confidence=0.85,
        recommended_investigation="Check payment API logs",
        severity="HIGH"
    )
    assert analysis.severity == "HIGH"
    assert analysis.confidence == 0.85


def test_confidence_range():
    analysis = RootCauseAnalysis(
        failed_check="expect_not_null",
        column="payment_method",
        explanation="Test",
        confidence=0.75,
        recommended_investigation="Check logs",
        severity="MEDIUM"
    )
    assert 0.0 <= analysis.confidence <= 1.0


def test_rule_type_is_string():
    rule = DQRule(
        column="customer_age",
        rule_type="value_range",
        parameters={"min_value": 18, "max_value": 100},
        reasoning="Age must be valid"
    )
    assert isinstance(rule.rule_type, str)


def test_llm_json_parsing():
    raw = '{"dataset_name": "test", "rules": [], "summary": "ok"}'
    ruleset = DQRuleSet.model_validate_json(raw)
    assert ruleset.dataset_name == "test"


def test_llm_json_with_backticks():
    raw = '```json\n{"dataset_name": "test", "rules": [], "summary": "ok"}\n```'
    cleaned = raw.replace("```json", "").replace("```", "").strip()
    ruleset = DQRuleSet.model_validate_json(cleaned)
    assert ruleset.dataset_name == "test"


def test_severity_values():
    for severity in ["HIGH", "MEDIUM", "LOW"]:
        analysis = RootCauseAnalysis(
            failed_check="test",
            column="test_col",
            explanation="Test explanation",
            confidence=0.8,
            recommended_investigation="Check logs",
            severity=severity
        )
        assert analysis.severity in ["HIGH", "MEDIUM", "LOW"]
```

Save with **Ctrl+S**.

---

## Step 2 — Create GitHub Actions Workflow

In VS Code, create this folder structure:
```
.github/
└── workflows/
    └── ci.yml