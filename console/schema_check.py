"""Compare Kafka messages and contracts with what Schema Registry holds.

Topics stay plain JSON (scripts/register-schemas.sh says why), so the
registry checks changes to each contract and this module checks the
messages: the console samples each topic's latest messages and reports,
as warnings for the Alerts tab, any that break the registered contract,
any contract change the registry has accepted (an evolution Flink's
sources may not have caught up with), and a repository contract that
differs from the registered one (refused as incompatible, or never
registered).

Flink reads these topics with 'json.ignore-parse-errors', so a message it
can't parse is dropped without a word; these warnings are the only place
that shows up.

Only the JSON Schema keywords the contracts in kafka/contracts use are
checked: type, const, properties, required and additionalProperties: false.
Pure functions, so they are tested without a registry or broker.
"""

from __future__ import annotations

import json

JSON_TYPES = {
    "string": lambda v: isinstance(v, str),
    "integer": lambda v: isinstance(v, int) and not isinstance(v, bool),
    "number": lambda v: isinstance(v, (int, float)) and not isinstance(v, bool),
    "boolean": lambda v: isinstance(v, bool),
    "null": lambda v: v is None,
    "object": lambda v: isinstance(v, dict),
    "array": lambda v: isinstance(v, list),
}


def validate(schema: dict, msg) -> list[str]:
    """What is wrong with one decoded message, or [] if nothing."""
    if not isinstance(msg, dict):
        return ["not a JSON object"]
    props = schema.get("properties", {})
    errors = [f"missing {name}" for name in schema.get("required", []) if name not in msg]
    for name, value in msg.items():
        rule = props.get(name)
        if rule is None:
            if schema.get("additionalProperties") is False:
                errors.append(f"unexpected field {name}")
            continue
        if "const" in rule and value != rule["const"]:
            errors.append(f"{name} must be {json.dumps(rule['const'])}, got {json.dumps(value)}")
        types = rule.get("type")
        if types:
            allowed = [types] if isinstance(types, str) else types
            if not any(JSON_TYPES[t](value) for t in allowed if t in JSON_TYPES):
                errors.append(f"{name} should be {' or '.join(allowed)}, got {type(value).__name__}")
    return errors


def diff_schemas(old: dict, new: dict) -> dict:
    """Fields added, removed and changed between two contracts."""
    a, b = old.get("properties", {}), new.get("properties", {})
    return {
        "added": sorted(set(b) - set(a)),
        "removed": sorted(set(a) - set(b)),
        "changed": sorted(k for k in set(a) & set(b) if a[k] != b[k]),
        "required_added": sorted(set(new.get("required", [])) - set(old.get("required", []))),
    }


def describe_diff(d: dict) -> str:
    parts = []
    for key, label in (
        ("added", "adds"),
        ("removed", "removes"),
        ("changed", "changes"),
        ("required_added", "now requires"),
    ):
        if d[key]:
            parts.append(f"{label} {', '.join(d[key])}")
    return "; ".join(parts) or "changes only its description"


def check_topic(topic: str, contract: dict | None, versions: list[dict], samples: list[bytes]) -> list[dict]:
    """Warnings for one topic.

    contract: the repository's kafka/contracts/<topic>.json, or None.
    versions: the registered versions, oldest first, each {"version", "schema"}
              with the schema already decoded; [] when the subject is absent.
    samples:  the raw values of the topic's latest messages.
    """
    out = []

    def warn(code: str, message: str, level: str = "warning", **extra):
        out.append({"topic": topic, "code": code, "level": level, "message": message, **extra})

    if not versions:
        warn(
            "not_registered",
            "No contract is registered, so nothing checks changes to this topic's messages. "
            "Run Register topic schemas.",
        )
        return out
    latest = versions[-1]
    registered = latest["schema"]
    if len(versions) > 1:
        diff = diff_schemas(versions[-2]["schema"], registered)
        warn(
            "evolved",
            f"Contract evolved to version {latest['version']}, which {describe_diff(diff)} "
            f"relative to version {versions[-2]['version']}. Check the Flink source columns "
            "in flink/sql/01_sources.sql read the new fields.",
            version=latest["version"],
            diff=diff,
        )
    if contract is not None and contract != registered:
        diff = diff_schemas(registered, contract)
        warn(
            "contract_differs",
            f"The repository contract differs from registered version {latest['version']}: "
            f"it {describe_diff(diff)}. Either it hasn't been registered yet or the registry "
            "refused it as incompatible; Register topic schemas says which.",
            diff=diff,
        )

    bad, examples = 0, []
    for raw in samples:
        try:
            errors = validate(registered, json.loads(raw))
        except (ValueError, TypeError):
            errors = ["not valid JSON"]
        if errors:
            bad += 1
            if len(examples) < 3:
                examples.append("; ".join(errors))
    if bad:
        warn(
            "invalid_messages",
            f"{bad} of the latest {len(samples)} messages break registered version "
            f"{latest['version']}. Flink drops a message it can't parse without logging it.",
            level="critical" if bad == len(samples) else "warning",
            examples=examples,
            sampled=len(samples),
            failed=bad,
        )
    return out
