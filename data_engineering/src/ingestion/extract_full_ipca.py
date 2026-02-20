import json
import requests

URL = "https://apisidra.ibge.gov.br/values/t/1737/v/2266/p/199501-202512/n1/1"

resp = requests.get(URL, timeout=30)
resp.raise_for_status()

data = resp.json()

with open("/home/maki/projects/ipca_project/ipca-data-pipeline/data/full_load.json", "w", encoding="utf-8") as f:
    json.dump(data, f, ensure_ascii=False)
