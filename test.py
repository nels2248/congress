import httpx
import os
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("CONGRESS_API_KEY")

congress = 119

url = f"https://api.congress.gov/v3/bill/{congress}"

params = {
    "api_key": API_KEY,
    "format": "json",
    "limit": 2,
    "sort": "updateDatet+desc"
}

r = httpx.get(url, params=params)
data = r.json()

print("\nLATEST LEGISLATIVE ACTIVITY\n")
print("-" * 80)

for b in data.get("bills", []):
    latest = b.get("latestAction") or {}

    action_date = latest.get("actionDate")
    action_text = latest.get("text", "")

    if not action_date:
        continue

    print(
        f"{action_date} | {b.get('type')} {b.get('number')} | {action_text[:70]}"
    )