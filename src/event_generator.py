import json
import uuid
import random
import time
from datetime import datetime, timezone
from kafka import KafkaProducer
import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

# --- DB Connection ---
conn = psycopg2.connect(
    host="localhost",
    port=5432,
    dbname=os.getenv("POSTGRES_DB"),
    user=os.getenv("POSTGRES_USER"),
    password=os.getenv("POSTGRES_PASSWORD")
)
cursor = conn.cursor()

# --- Kafka Producer ---
producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# --- Load reference data from PostgreSQL ---
def load_reference_data():
    cursor.execute("""
        SELECT DISTINCT c.customer_unique_id, o.order_id, oi.product_id,
               p.product_category_name, oi.price, op.payment_type
        FROM bronze.customers c
        JOIN bronze.orders o ON c.customer_id = o.customer_id
        JOIN bronze.order_items oi ON o.order_id = oi.order_id
        JOIN bronze.products p ON oi.product_id = p.product_id
        LEFT JOIN bronze.order_payments op ON o.order_id = op.order_id
        LIMIT 5000
    """)
    return cursor.fetchall()

# --- Helpers ---
PLATFORMS = ["web", "mobile_ios", "mobile_android"]
DEVICES = ["desktop", "tablet", "mobile"]
OS_MAP = {"web": "Windows", "mobile_ios": "iOS", "mobile_android": "Android"}
PAGES = ["homepage", "category_page", "search_results", "deals_page"]
REFERRERS = ["https://google.com", "https://instagram.com", "direct", "https://email.dicks.com"]

def make_envelope(event_type, customer_id, session_id, platform, device):
    return {
        "_id": str(uuid.uuid4()),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "eventType": event_type,
        "identityMap": {
            "customerId": [{"id": customer_id, "primary": True}]
        },
        "environment": {
            "type": "browser" if platform == "web" else "application",
            "operatingSystem": OS_MAP[platform],
            "deviceCategory": device
        },
        "web": {
            "webSession": {"ID": session_id}
        }
    }

def page_view_event(customer_id, session_id, platform, device):
    event = make_envelope("web.webpagedetails.pageViews", customer_id, session_id, platform, device)
    event["web"]["webPageDetails"] = {"name": random.choice(PAGES), "URL": "/"}
    event["web"]["webReferrer"] = {"URL": random.choice(REFERRERS), "type": "external"}
    return event

def product_view_event(customer_id, session_id, platform, device, product_id, category, price):
    event = make_envelope("commerce.productViews", customer_id, session_id, platform, device)
    event["productListItems"] = [{"SKU": product_id, "name": category, "priceTotal": float(price), "quantity": 1}]
    event["commerce"] = {"productViews": {"value": 1}}
    return event

def add_to_cart_event(customer_id, session_id, platform, device, product_id, category, price):
    event = make_envelope("commerce.productListAdds", customer_id, session_id, platform, device)
    event["productListItems"] = [{"SKU": product_id, "name": category, "priceTotal": float(price), "quantity": 1}]
    event["commerce"] = {"productListAdds": {"value": 1}}
    return event

def remove_from_cart_event(customer_id, session_id, platform, device, product_id):
    event = make_envelope("commerce.productListRemovals", customer_id, session_id, platform, device)
    event["productListItems"] = [{"SKU": product_id, "name": "", "priceTotal": 0.0, "quantity": 1}]
    event["commerce"] = {"productListRemovals": {"value": 1}}
    return event

def purchase_event(customer_id, session_id, platform, device, order_id, items, total, payment_type):
    event = make_envelope("commerce.purchases", customer_id, session_id, platform, device)
    event["productListItems"] = items
    event["commerce"] = {
        "purchases": {"value": 1},
        "order": {
            "purchaseID": order_id,
            "priceTotal": float(total),
            "currencyCode": "BRL",
            "payments": [{"paymentType": payment_type or "credit_card"}]
        }
    }
    return event

# --- Session Simulator ---
def simulate_session(order_rows):
    """Simulate one browsing session from a group of order rows (same order_id)."""
    customer_id = order_rows[0][0]
    order_id = order_rows[0][1]
    platform = random.choice(PLATFORMS)
    device = random.choice(DEVICES)
    session_id = str(uuid.uuid4())
    events = []

    # Step 1: page view
    events.append(page_view_event(customer_id, session_id, platform, device))
    time.sleep(0.05)

    # Step 2: product views
    for row in order_rows:
        _, _, product_id, category, price, _ = row
        events.append(product_view_event(customer_id, session_id, platform, device, product_id, category, price))
        time.sleep(0.05)

    # Step 3: add to cart
    cart_items = []
    for row in order_rows:
        _, _, product_id, category, price, _ = row
        events.append(add_to_cart_event(customer_id, session_id, platform, device, product_id, category, price))
        cart_items.append({"SKU": product_id, "name": category, "priceTotal": float(price), "quantity": 1})
        time.sleep(0.05)

    # Step 4: occasional remove from cart
    if random.random() < 0.15 and len(cart_items) > 1:
        removed = random.choice(order_rows)
        events.append(remove_from_cart_event(customer_id, session_id, platform, device, removed[2]))
        cart_items = [i for i in cart_items if i["SKU"] != removed[2]]
        time.sleep(0.05)

    # Step 5: purchase
    total = sum(i["priceTotal"] for i in cart_items)
    payment_type = order_rows[0][5]
    events.append(purchase_event(customer_id, session_id, platform, device, order_id, cart_items, total, payment_type))

    return events

def simulate_browse_only_session():
    """Simulate a session with no purchase — realistic ~95% non-converting traffic."""
    customer_id = str(uuid.uuid4())
    session_id = str(uuid.uuid4())
    platform = random.choice(PLATFORMS)
    device = random.choice(DEVICES)
    events = []

    events.append(page_view_event(customer_id, session_id, platform, device))
    for _ in range(random.randint(1, 4)):
        product_id = str(uuid.uuid4())
        category = random.choice(["sports", "fitness", "outdoor", "footwear"])
        price = round(random.uniform(20, 500), 2)
        events.append(product_view_event(customer_id, session_id, platform, device, product_id, category, price))
        time.sleep(0.05)

    return events

# --- Main ---
def main():
    print("Loading reference data from PostgreSQL...")
    rows = load_reference_data()

    # Group rows by order_id
    orders = {}
    for row in rows:
        order_id = row[1]
        orders.setdefault(order_id, []).append(row)

    print(f"Loaded {len(orders)} orders. Starting event generation...")

    for order_id, order_rows in orders.items():
        # Simulate purchasing session
        events = simulate_session(order_rows)
        for event in events:
            producer.send("olist.events", value=event)

        # Simulate browse-only sessions (ratio ~19:1 to get ~5% conversion)
        for _ in range(random.randint(15, 25)):
            browse_events = simulate_browse_only_session()
            for event in browse_events:
                producer.send("olist.events", value=event)

        time.sleep(0.1)

    producer.flush()
    print("Done. All events sent to Kafka topic: olist.events")

if __name__ == "__main__":
    main()