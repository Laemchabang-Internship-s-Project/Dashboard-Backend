import asyncio  
import json  
import redis.asyncio as redis  
import os  
from dotenv import load_dotenv  
load_dotenv()  
from cache_graph import fetch_doctor_operations_stats_sync  
async def main():  
    client = redis.Redis(host=os.getenv('REDIS_HOST', 'localhost'), port=int(os.getenv('REDIS_PORT', 6379)), password=os.getenv('REDIS_PASSWORD'), decode_responses=True)  
    stats = fetch_doctor_operations_stats_sync()  
    await client.set('graph_doctor_stats_oper', json.dumps(stats.get('operations', []), ensure_ascii=False))  
    await client.set('graph_doctor_stats_doc', json.dumps(stats.get('doctors', []), ensure_ascii=False))  
    await client.set('graph_doctor_stats_dept', json.dumps(stats.get('departments', []), ensure_ascii=False))  
    await client.set('graph_doctor_stats_drilldown', json.dumps(stats.get('drilldown', []), ensure_ascii=False))  
    print('Saved to Redis!')  
    await client.close()  
asyncio.run(main())  
