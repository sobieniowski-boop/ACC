import asyncio
import sys
import os

# Set up paths
os.chdir(r'C:\ACC')
sys.path.insert(0, r'C:\ACC\apps\api')
os.environ['PYTHONPATH'] = r'C:\ACC\apps\api'

from app.services.listings_client import ListingsClient

async def check():
    client = ListingsClient('A13V1IB3VIYZZH')  # FR marketplace
    data = await client.get_listings_item('A1O0H08K2DYVHX', 'FBA_5902730382102')
    
    print('SKU: FBA_5902730382102')
    print(f'Product type: {data.get("productType")}')
    
    attrs = data.get('attributes', {})
    print(f'\nHas child_parent_sku_relationship: {"child_parent_sku_relationship" in attrs}')
    print(f'Has parentage_level: {"parentage_level" in attrs}')
    
    if 'child_parent_sku_relationship' in attrs:
        print(f'Value: {attrs["child_parent_sku_relationship"]}')
    if 'parentage_level' in attrs:
        print(f'Parentage: {attrs["parentage_level"]}')
    
    print(f'\nAll attribute keys ({len(attrs)} total):')
    for i, key in enumerate(list(attrs.keys())[:30]):
        print(f'  {i+1}. {key}')

asyncio.run(check())
