import asyncio
from dotenv import load_dotenv
load_dotenv()
from cache_graph import task_update_depression

async def main():
    print("Running task_update_depression manually...")
    try:
        task = asyncio.create_task(task_update_depression())
        await asyncio.sleep(60) # wait 60s
        task.cancel()
        print("Done running task.")
    except Exception as e:
        print("Error:", e)

if __name__ == "__main__":
    asyncio.run(main())
