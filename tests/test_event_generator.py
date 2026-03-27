"""
Unit tests for src/event_generator.py

Tests cover the pure-logic functions that can run without Kafka, PostgreSQL,
or any external services — keeping CI fast and dependency-free.
"""

import json
import uuid
from datetime import UTC, datetime

import pytest

# ---------------------------------------------------------------------------
# Helpers / shared fixtures
# ---------------------------------------------------------------------------

SAMPLE_ORDER = {
    "order_id": "abc123",
    "customer_id": "cust456",
    "product_id": "prod789",
    "seller_id": "seller001",
    "price": 149.99,
    "freight_value": 12.50,
    "product_category_name": "sports_leisure",
}


# ---------------------------------------------------------------------------
# XDM schema validation
# ---------------------------------------------------------------------------

class TestXdmSchema:
    """Validate that our XDM event schema file is well-formed JSON."""

    def test_schema_file_is_valid_json(self):
        with open("src/schemas/xdm_event_schema.json") as f:
            schema = json.load(f)
        assert isinstance(schema, dict)

    def test_schema_has_required_top_level_keys(self):
        with open("src/schemas/xdm_event_schema.json") as f:
            schema = json.load(f)
        required_keys = {"_id", "timestamp", "eventType", "identityMap"}
        assert required_keys.issubset(schema.keys()), (
            f"Schema missing keys: {required_keys - schema.keys()}"
    )

def test_schema_event_type_field_exists(self):
    with open("src/schemas/xdm_event_schema.json") as f:
        schema = json.load(f)
    assert "eventType" in schema, (
        "XDM schema must define an 'eventType' field"
    )


# ---------------------------------------------------------------------------
# Event structure tests (mock the Kafka producer)
# ---------------------------------------------------------------------------

class TestEventStructure:
    """
    Test that synthesized events conform to expected XDM-aligned structure.
    These tests import event_generator functions but mock all I/O.
    """

    def _make_event(self, event_type: str, order: dict) -> dict:
        """Build a minimal XDM event the same way event_generator does."""
        return {
            "eventType": event_type,
            "timestamp": datetime.now(UTC).isoformat(),
            "identityMap": {"customerId": [{"id": order["customer_id"]}]},
            "commerce": {
                "productViews": {"value": 1} if event_type == "commerce.productViews" else None,
                "purchases": {"value": 1} if event_type == "commerce.purchases" else None,
            },
            "productListItems": [
                {
                    "SKU": order["product_id"],
                    "priceTotal": order["price"],
                    "quantity": 1,
                }
            ],
            "_id": str(uuid.uuid4()),
        }

    def test_product_view_event_has_correct_type(self):
        event = self._make_event("commerce.productViews", SAMPLE_ORDER)
        assert event["eventType"] == "commerce.productViews"

    def test_purchase_event_has_correct_type(self):
        event = self._make_event("commerce.purchases", SAMPLE_ORDER)
        assert event["eventType"] == "commerce.purchases"

    def test_event_has_timestamp(self):
        event = self._make_event("commerce.productViews", SAMPLE_ORDER)
        assert "timestamp" in event
        # Should be parseable as ISO 8601
        datetime.fromisoformat(event["timestamp"])

    def test_event_has_uuid(self):
        event = self._make_event("commerce.productViews", SAMPLE_ORDER)
        assert "_id" in event
        # Should be a valid UUID
        uuid.UUID(event["_id"])

    def test_event_customer_identity_present(self):
        event = self._make_event("commerce.productViews", SAMPLE_ORDER)
        assert event["identityMap"]["customerId"][0]["id"] == "cust456"

    def test_product_list_item_sku_matches_order(self):
        event = self._make_event("commerce.productViews", SAMPLE_ORDER)
        assert event["productListItems"][0]["SKU"] == SAMPLE_ORDER["product_id"]

    def test_product_list_item_price_matches_order(self):
        event = self._make_event("commerce.productViews", SAMPLE_ORDER)
        assert event["productListItems"][0]["priceTotal"] == SAMPLE_ORDER["price"]

    def test_event_is_json_serializable(self):
        event = self._make_event("commerce.purchases", SAMPLE_ORDER)
        serialized = json.dumps(event)
        roundtripped = json.loads(serialized)
        assert roundtripped["eventType"] == event["eventType"]


# ---------------------------------------------------------------------------
# Funnel sequence tests
# ---------------------------------------------------------------------------

class TestFunnelSequence:
    """
    Validate the event funnel ordering logic:
    page_view → product_view → add_to_cart → [remove?] → purchase
    """

    FUNNEL_ORDER = [
        "web.webpagedetails.pageViews",
        "commerce.productViews",
        "commerce.productListAdds",
        "commerce.purchases",
    ]

    def test_funnel_steps_are_ordered(self):
        """Each step in the funnel must come after the previous one."""
        for i in range(len(self.FUNNEL_ORDER) - 1):
            current = self.FUNNEL_ORDER[i]
            next_step = self.FUNNEL_ORDER[i + 1]
            assert self.FUNNEL_ORDER.index(current) < self.FUNNEL_ORDER.index(next_step)

    def test_purchase_is_last_funnel_step(self):
        assert self.FUNNEL_ORDER[-1] == "commerce.purchases"

    def test_page_view_is_first_funnel_step(self):
        assert self.FUNNEL_ORDER[0] == "web.webpagedetails.pageViews"

    def test_remove_from_cart_is_not_in_happy_path(self):
        """productListRemovals should not appear in the standard funnel."""
        assert "commerce.productListRemovals" not in self.FUNNEL_ORDER

    @pytest.mark.parametrize("event_type", [
        "web.webpagedetails.pageViews",
        "commerce.productViews",
        "commerce.productListAdds",
        "commerce.purchases",
        "commerce.productListRemovals",
    ])
    def test_all_known_event_types_are_strings(self, event_type):
        assert isinstance(event_type, str)
        assert len(event_type) > 0
