import requests
import pandas as pd
from io import StringIO

urls = {
    "orders": "https://etl-server.fly.dev/orders",
    "order_items": "https://etl-server.fly.dev/order_items",
    "customers": "https://etl-server.fly.dev/customers"
}

prefix = "csvs_from_api/"
for name, url in urls.items():
    response = requests.get(url)
    response.raise_for_status()  # Ensure we got a successful response
    data = StringIO(response.text)
    df = pd.read_json(data)
    df.to_csv(f"{prefix}{name}.csv", index=False)