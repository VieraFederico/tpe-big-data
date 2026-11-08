from dataclasses import dataclass
from functools import reduce

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

RULE_ID_COL = "_rule_id"
REASON_COL = "_reason"
QUARANTINE_TS_COL = "_quarantine_ts"


@dataclass(frozen=True)
class Rule:
    rule_id: str
    reason: str
    violation: Column


def split_by_rules(df: DataFrame, rules: list[Rule]) -> tuple[DataFrame, DataFrame]:
    if not rules:
        return df, df.limit(0)

    rule_id = reduce(
        lambda acc, r: acc.when(F.coalesce(r.violation, F.lit(False)), F.lit(r.rule_id)),
        rules[1:],
        F.when(F.coalesce(rules[0].violation, F.lit(False)), F.lit(rules[0].rule_id)),
    )
    reasons = F.create_map(*[x for r in rules for x in (F.lit(r.rule_id), F.lit(r.reason))])

    tagged = df.withColumn(RULE_ID_COL, rule_id)
    valid = tagged.filter(F.col(RULE_ID_COL).isNull()).drop(RULE_ID_COL)
    quarantine = (
        tagged.filter(F.col(RULE_ID_COL).isNotNull())
        .withColumn(REASON_COL, reasons[F.col(RULE_ID_COL)])
        .withColumn(QUARANTINE_TS_COL, F.current_timestamp())
    )
    return valid, quarantine


def any_null(columns: tuple[str, ...] | list[str]) -> Column:
    return reduce(lambda acc, c: acc | F.col(c).isNull(), columns[1:], F.col(columns[0]).isNull())
