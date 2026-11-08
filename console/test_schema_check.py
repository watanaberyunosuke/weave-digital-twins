import json
import unittest
from pathlib import Path

from schema_check import check_topic, diff_schemas, validate

CONTRACTS = Path(__file__).resolve().parent.parent / "kafka" / "contracts"


def contract(topic: str) -> dict:
    return json.loads((CONTRACTS / f"{topic}.json").read_text())


METER = "water.meter-readings.raw.v1"
GOOD_METER = {
    "meter_id": "D01-M001",
    "dma_id": "D01",
    "litres": 12.5,
    "flow_lpm": 0.833,
    "battery_pct": 97,
    "event_ts": 1767225600000,
    "is_synthetic": True,
}


class ValidateTests(unittest.TestCase):
    def test_a_producer_message_passes(self):
        self.assertEqual(validate(contract(METER), GOOD_METER), [])

    def test_hydrometry_envelope_passes(self):
        msg = {
            "source": "kiwis",
            "ingest_ts": "2026-11-08T00:00:00+00:00",
            "event_ts": "2026-11-07T23:00:00+00:00",
            "is_synthetic": False,
            "site_id": "S1",
            "variable": "level",
            "unit": "m",
            "value": None,
            "quality_code": "",
            "provider": "kiwis",
        }
        self.assertEqual(validate(contract("water.hydrometry.raw.v1"), msg), [])

    def test_real_feed_marked_synthetic_is_caught(self):
        msg = {"source": "x", "event_ts": "t", "is_synthetic": True, "site_id": "S", "variable": "v"}
        msg |= {"value": 1.0, "provider": "x"}
        errors = validate(contract("water.hydrometry.raw.v1"), msg)
        self.assertIn("is_synthetic must be false, got true", errors)

    def test_missing_wrong_type_and_extra_fields(self):
        msg = {k: v for k, v in GOOD_METER.items() if k != "dma_id"} | {"litres": "12", "firmware": "1.2"}
        errors = validate(contract(METER), msg)
        self.assertIn("missing dma_id", errors)
        self.assertIn("litres should be number, got str", errors)
        self.assertIn("unexpected field firmware", errors)

    def test_bool_is_not_an_integer(self):
        self.assertTrue(validate(contract(METER), GOOD_METER | {"battery_pct": True}))


class CheckTopicTests(unittest.TestCase):
    def versions(self, *schemas):
        return [{"version": i + 1, "schema": s} for i, s in enumerate(schemas)]

    def codes(self, warnings):
        return [w["code"] for w in warnings]

    def test_clean_topic_has_no_warnings(self):
        c = contract(METER)
        samples = [json.dumps(GOOD_METER).encode()] * 3
        self.assertEqual(check_topic(METER, c, self.versions(c), samples), [])

    def test_unregistered_topic(self):
        self.assertEqual(self.codes(check_topic(METER, contract(METER), [], [])), ["not_registered"])

    def test_evolution_is_reported_with_its_diff(self):
        old = contract(METER)
        new = json.loads(json.dumps(old))
        new["properties"]["firmware"] = {"type": "string"}
        warnings = check_topic(METER, new, self.versions(old, new), [])
        self.assertEqual(self.codes(warnings), ["evolved"])
        self.assertEqual(warnings[0]["diff"]["added"], ["firmware"])
        self.assertEqual(warnings[0]["version"], 2)

    def test_repository_contract_ahead_of_registry(self):
        registered = contract(METER)
        repo = json.loads(json.dumps(registered))
        del repo["properties"]["battery_pct"]
        warnings = check_topic(METER, repo, self.versions(registered), [])
        self.assertEqual(self.codes(warnings), ["contract_differs"])
        self.assertEqual(warnings[0]["diff"]["removed"], ["battery_pct"])

    def test_bad_messages_are_counted_and_all_bad_is_critical(self):
        c = contract(METER)
        some = [json.dumps(GOOD_METER).encode(), b"not json", json.dumps({"meter_id": 1}).encode()]
        w = check_topic(METER, c, self.versions(c), some)
        self.assertEqual(self.codes(w), ["invalid_messages"])
        self.assertEqual((w[0]["failed"], w[0]["sampled"], w[0]["level"]), (2, 3, "warning"))
        self.assertIn("not valid JSON", w[0]["examples"])
        all_bad = check_topic(METER, c, self.versions(c), [b"{}"])
        self.assertEqual(all_bad[0]["level"], "critical")


class DiffTests(unittest.TestCase):
    def test_required_added(self):
        a = {"properties": {"x": {"type": "string"}}, "required": []}
        b = {"properties": {"x": {"type": "string"}}, "required": ["x"]}
        self.assertEqual(diff_schemas(a, b)["required_added"], ["x"])


if __name__ == "__main__":
    unittest.main()
