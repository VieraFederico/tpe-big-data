from dataclasses import dataclass
from functools import reduce

from pyspark.sql import Column, DataFrame
from pyspark.sql.functions import coalesce, col, create_map, current_timestamp, lit, when

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
        lambda expression, rule: expression.when(coalesce(rule.violation, lit(False)), lit(rule.rule_id)),
        rules[1:],
        when(coalesce(rules[0].violation, lit(False)), lit(rules[0].rule_id)),
    )
    reasons = create_map(*[item for rule in rules for item in (lit(rule.rule_id), lit(rule.reason))])

    tagged = df.withColumn(RULE_ID_COL, rule_id)
    valid = tagged.filter(col(RULE_ID_COL).isNull()).drop(RULE_ID_COL)
    quarantine = (
        tagged.filter(col(RULE_ID_COL).isNotNull())
        .withColumn(REASON_COL, reasons[col(RULE_ID_COL)])
        .withColumn(QUARANTINE_TS_COL, current_timestamp())
    )
    return valid, quarantine


def any_null(columns: tuple[str, ...] | list[str]) -> Column:
    return reduce(lambda condition, name: condition | col(name).isNull(), columns[1:], col(columns[0]).isNull())
