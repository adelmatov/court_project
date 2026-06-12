import requests
import json
from pathlib import Path

TOKEN = '4dfbf650-ef3b-412a-8f4c-5dfc33bcea89'
API_URL = 'https://portal.kgd.gov.kz/services/isnaportalsync/public/taxpayer-data'
BIN = '190140001652'

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36 Edg/144.0.0.0",
    "Accept": "application/json",
    "X-Portal-Token": TOKEN,
}

params = {'taxpayerCode': BIN, 'taxpayerType': 'UL'}

response = requests.get(API_URL, headers=HEADERS, params=params, timeout=30)

try:
    body = response.json()
except Exception:
    body = response.text

result = {
    "url": response.url,
    "status_code": response.status_code,
    "reason": response.reason,
    "elapsed_sec": response.elapsed.total_seconds(),
    "request_headers": dict(response.request.headers),
    "response_headers": dict(response.headers),
    "cookies": dict(response.cookies),
    "body": body,
}

output_file = Path(__file__).parent / f"response_{BIN}.json"
with open(output_file, "w", encoding="utf-8") as f:
    json.dump(result, f, ensure_ascii=False, indent=2)

print(f"✅ Сохранено: {output_file}")
print(f"HTTP {response.status_code} {response.reason}")
print(json.dumps(body, ensure_ascii=False, indent=2) if isinstance(body, (dict, list)) else body)