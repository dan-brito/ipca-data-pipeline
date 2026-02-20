import requests
import json

URL = "https://apisidra.ibge.gov.br/values/t/7060/v/63/p/all/n6/all/n7/all?formato=json"

resp = requests.get(URL, timeout=30)
resp.raise_for_status()

data = resp.json()
()
with open("/home/maki/projects/ipca_project/ipca-data-pipeline/data/full_load_by_region.json", "w", encoding="utf-8") as f:
    json.dump(data, f, ensure_ascii=False)