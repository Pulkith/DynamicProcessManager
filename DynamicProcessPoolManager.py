from typing import Any, List, Tuple
from uuid import UUID
import asyncio
from EnhancedFuture import EnhancedFuture
from PoolTask import PoolTask
from DynamicPoolConfig import DynamicPoolConfig
from _DynamicProcessPoolStateInstance import _DynamicProcessPoolStateInstance



#########################################################################################################
#########################################################################################################
#####  DynamicProcessPoolManager (Public)
#### -  This class is used to expose the API for the dynamic process pool manager
#########################################################################################################
#########################################################################################################
class DynamicProcessPoolManager:

    #########################################################################################################
    #####  Initialization
    #########################################################################################################
    
    def __init__(self, config: DynamicPoolConfig) -> None:
        self._config = config
        self.pool_manager: _DynamicProcessPoolStateInstance = _DynamicProcessPoolStateInstance(self._config)
   
    #########################################################################################################
    #####  Single Tasks
    #########################################################################################################
  
    async def add_task(self, task: PoolTask) -> Tuple[UUID, EnhancedFuture]:
        return await self.pool_manager.add_task(task)
    
    async def add_task_wait(self, task: PoolTask) -> Any: 
        task_id_and_future: Tuple[UUID, EnhancedFuture] = await self.add_task(task)
        result: Any = await task_id_and_future[1]
        return result
    
    #########################################################################################################
    #####  Batch Tasks
    #########################################################################################################

    async def add_batch_tasks(self, tasks: List[PoolTask]) -> List[Tuple[UUID, EnhancedFuture]]:
        if len(tasks) == 0:
            return []

        different_pools: bool = len(set([task.pool_type for task in tasks])) > 1
        different_users: bool = len(set([task.user_id for task in tasks])) > 1

        if (not different_pools) and (not different_users):
            return await self.pool_manager.add_batch_tasks_of_same_user_same_pool(tasks)
        elif (not different_pools) and different_users:
            return await self.pool_manager.add_batch_tasks_same_user_different_pools(tasks)
        else:
            raise ValueError("Not Implemented: Cannot add batch tasks of different pools and different users")
    
    async def add_batch_tasks_wait(self, tasks: List[PoolTask]) -> List[Any]:
        ids_and_futures: List[Tuple[UUID, EnhancedFuture]] = await self.add_batch_tasks(tasks)
        results: List[Any] = await asyncio.gather(*[future[1] for future in ids_and_futures])
        return results
        
    #########################################################################################################
    #####  Shutdown
    #########################################################################################################
   
    async def close_all_pools(self) -> None:
        await self.pool_manager.terminate()
    
    #########################################################################################################
    #####  Wait on User Futures
    #########################################################################################################
   
    async def wait_on_all_futures_of_user(self, user_id: str) -> None:
        await self.pool_manager.wait_on_all_futures_of_user(user_id)

    #########################################################################################################
    #####  Context Manager
    #########################################################################################################
    
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        asyncio.run(self.close_all_pools()) #not best practcice?
