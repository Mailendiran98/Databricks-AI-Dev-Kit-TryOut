# Databricks Python notebook
# Ingest all people from SWAPI into swapi_demo.raw_data.characters

import json
import urllib.request
from pyspark.sql import Row

rows = []
url = 'https://swapi.dev/api/people/'

while url:
    with urllib.request.urlopen(url) as resp:
        data = json.load(resp)
    for p in data.get('results', []):
        rows.append({
            'name': p.get('name'),
            'height': p.get('height'),
            'mass': p.get('mass'),
            'birth_year': p.get('birth_year'),
            'homeworld': p.get('homeworld')
        })
    url = data.get('next')

if rows:
    df = spark.createDataFrame([Row(**r) for r in rows])
    df = df.select('name','height','mass','birth_year','homeworld')
    df.write.format('delta').mode('append').saveAsTable('swapi_demo.raw_data.characters')
    print(f"Wrote {df.count()} rows to swapi_demo.raw_data.characters")
else:
    print('No rows fetched from SWAPI')
