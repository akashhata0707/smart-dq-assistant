from dataclasses import dataclass, field
from typing import Any
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType
from datetime import datetime


@dataclass
class DQResult:
    check_name: str
    column: str
    rule_type: str
    success: bool
    metric_value: Any
    threshold: Any
    timestamp: str = field(default_factory=lambda: datetime.now().isoformat())


class DQEngine:
    def __init__(self, df: DataFrame, dataset_name: str = "dataset"):
        self.df = df
        self.dataset_name = dataset_name
        self.results = []
        self.total_rows = df.count()

    def expect_not_null(self, column: str) -> DQResult:
        null_count = self.df.filter(F.col(column).isNull()).count()
        null_rate = round(null_count / self.total_rows, 4)
        r = DQResult("expect_not_null", column, "not_null",
                     null_count == 0, null_rate, 0.0)
        self.results.append(r)
        return r

    def expect_unique(self, column: str) -> DQResult:
        distinct_count = self.df.select(column).distinct().count()
        r = DQResult("expect_unique", column, "unique",
                     distinct_count == self.total_rows, distinct_count, self.total_rows)
        self.results.append(r)
        return r

    def expect_value_range(self, column: str, min_value=None, max_value=None) -> DQResult:
        condition = F.lit(True)
        if min_value is not None:
            condition = condition & (F.col(column) >= min_value)
        if max_value is not None:
            condition = condition & (F.col(column) <= max_value)
        fail_count = self.df.filter(~condition).count()
        fail_rate = round(fail_count / self.total_rows, 4)
        r = DQResult("expect_value_range", column, "value_range",
                     fail_count == 0, fail_rate, 0.0)
        self.results.append(r)
        return r

    def expect_accepted_values(self, column: str, accepted: list) -> DQResult:
        fail_count = self.df.filter(~F.col(column).isin(accepted)).count()
        fail_rate = round(fail_count / self.total_rows, 4)
        r = DQResult("expect_accepted_values", column, "accepted_values",
                     fail_count == 0, fail_rate, 0.0)
        self.results.append(r)
        return r

    def expect_null_rate_below(self, column: str, max_null_rate: float) -> DQResult:
        null_count = self.df.filter(F.col(column).isNull()).count()
        null_rate = round(null_count / self.total_rows, 4)
        r = DQResult("expect_null_rate_below", column, "null_rate",
                     null_rate <= max_null_rate, null_rate, max_null_rate)
        self.results.append(r)
        return r

    def summary(self):
        print(f"\n--- DQ Results: {self.dataset_name} ---")
        pass_count = 0
        for r in self.results:
            status = "✅ PASS" if r.success else "❌ FAIL"
            print(f"{status} | {r.column} | {r.rule_type} | metric={r.metric_value}")
            if r.success:
                pass_count += 1
        print(f"\nTotal: {pass_count}/{len(self.results)} checks passed")
        return self.results