import asyncio
import time
import multiprocessing as mp
from PoolType import PoolType
from DynamicProcessPoolManager import DynamicProcessPoolManager
from DynamicPoolConfig import DynamicPoolConfig
from PoolTask import PoolTask


# Example usage
def example_sync_task(x):
    time.sleep(3)
    # print(x ** x)
    print(x)

async def example_async_task(x):
    await asyncio.sleep(3)
    print(x * x)
    return 0

async def main() -> None:
    config: DynamicPoolConfig = DynamicPoolConfig()
    config.set_config(PoolType.HEAVY, 3, 2)
    config.set_config(PoolType.EXPRESS, 1, 2)
    manager: DynamicProcessPoolManager = DynamicProcessPoolManager(config=config)
    CPU_COUNT: int = mp.cpu_count()

    # await manager.add_task(PoolTask("user1", PoolType.HEAVY, None, example_sync_task, None, 5))
    # await manager.add_task(PoolTask("user1", PoolType.HEAVY, None, example_sync_task, None, 5))
    # await manager.add_task(PoolTask("user1", PoolType.HEAVY, None, example_sync_task, None, 5))
    # await manager.add_task(PoolTask("user2", PoolType.HEAVY, None, example_sync_task, None, 5))

    await manager.add_task(PoolTask("user1", PoolType.HEAVY, None, example_sync_task, None, 1))
    await manager.add_task(PoolTask("user1", PoolType.HEAVY, None, example_sync_task, None, 1))
    await manager.add_task(PoolTask("user1", PoolType.HEAVY, None, example_sync_task, None, 1))
    await manager.add_task(PoolTask("user1", PoolType.HEAVY, None, example_sync_task, None, 1))

    await manager.add_task(PoolTask("user2", PoolType.HEAVY, None, example_sync_task, None, 2))
    await manager.add_task(PoolTask("user2", PoolType.HEAVY, None, example_sync_task, None, 2))

    await manager.add_task(PoolTask("user3", PoolType.HEAVY, None, example_sync_task, None, 3))
    await manager.add_task(PoolTask("user3", PoolType.HEAVY, None, example_sync_task, None, 3))

    await manager.add_task(PoolTask("user4", PoolType.HEAVY, None, example_sync_task, None, 4))

    # await manager.add_task(PoolTask("user10", PoolType.EXPRESS, None, example_sync_task, None, 10))
    # await manager.add_task(PoolTask("user11", PoolType.EXPRESS, None, example_sync_task, None, 11))
    # await manager.add_task(PoolTask("user12", PoolType.EXPRESS, None, example_sync_task, None, 12))

    await manager.close_all_pools()


    # await asyncio.sleep(10)



if __name__ == '__main__':
    asyncio.run(main())


