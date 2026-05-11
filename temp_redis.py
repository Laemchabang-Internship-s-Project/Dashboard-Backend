import asyncio  
import json  
import redis.asyncio as redis  
import os  
from dotenv import load_dotenv  
load_dotenv()  
async def main():  
    client = redis.Redis(host=os.getenv('REDIS_HOST', 'localhost'), port=int(os.getenv('REDIS_PORT', 6379)), password=os.getenv('REDIS_PASSWORD'), decode_responses=True)  
    raw = await client.get('graph_doctor_stats_oper')  
    if raw:  
        data = json.loads(raw)  
        print(f'Total items in redis: {len(data)}')  
        yr26 = [x for x in data if x.get('year') == '2026']  
        print(f'Items in 2026: {len(yr26)}')  
        print(yr26[:3])  
    else:  
        print('No data in Redis')  
    await client.close()  
asyncio.run(main())  
