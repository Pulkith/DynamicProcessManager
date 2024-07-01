import asyncio
from typing import List, Tuple, Dict, Optional, Set, cast
from uuid import UUID
from EnhancedFuture import EnhancedFuture
from PoolTask import PoolTask
from DynamicPoolConfig import DynamicPoolConfig
from _PoolWrapper import _PoolWrapper
from _ProcessPoolUser import _ProcessPoolUser
from PoolType import PoolType
from _PoolTypeMapping import _PoolTypeMapping


#########################################################################################################
#########################################################################################################
#####  _DynamicProcessPoolStateInstance (Private):                                                  #####
##### - A class to keep track of the state of the dynamic process pool.                             #####
#########################################################################################################
#########################################################################################################
class _DynamicProcessPoolStateInstance:
    #########################################################################################################
    #####  Init
    #########################################################################################################
   
    def __init__(self, config: DynamicPoolConfig) -> None:
        self.config: DynamicPoolConfig = config

        self.pools: dict[UUID, _PoolWrapper] = {}
        self.pool_of_type_mapping: _PoolTypeMapping = _PoolTypeMapping()

        self.users: dict[str, _ProcessPoolUser] = {}

        self._create_pools()
   
    #########################################################################################################
    #####  Create Pools
    #########################################################################################################
    
    def _register_pool(self, type: PoolType, cores: int) -> None:
        pool: _PoolWrapper = _PoolWrapper(type, cores)
        pool_id: UUID = pool.get_pool_id()
        self.pools[pool_id] = pool
        self.pool_of_type_mapping.add_pool(pool_id, type)

    def _create_pools(self) -> None:
        pool_types: List[PoolType]= self.config.get_types()
        for pool_type in pool_types:
            num_pools_of_type: int = self.config.get_config_of_type(pool_type)['num_pools_of_type']
            num_cores_per_pool: int= self.config.get_config_of_type(pool_type)['num_cores_per_pool']
            for _ in range(num_pools_of_type):
                self._register_pool(pool_type, num_cores_per_pool)
    
    #########################################################################################################
    #####  User Management
    #########################################################################################################

    #TODO: helper to add batch different POOL types AND different Pool Users??
    #TODO: add tasks as a batch (combined larger task)
    
    async def _assign_user_to_pool(self, user_id: str, pool_type: PoolType, pool_id: UUID) -> None:
        self.users[user_id].reassign_pool(pool_type, pool_id)
        self.pools[pool_id].add_user(user_id)

    async def _register_user(self, user_id: str) -> None:
        self.users[user_id] = _ProcessPoolUser(user_id)
        for type in self._get_active_pool_types():
            await self._assign_user_to_best_pool(user_id, type)

    async def _user_is_registered(self, user_id: str) -> bool:
        return (user_id in self.users and self.users[user_id] is not None)
    
    async def _reassign_user(self, user_id: str, pool_type: PoolType, best_pool: UUID) -> None:
        cur_pool: Optional[UUID] = self.users[user_id].get_pool(pool_type)
        if cur_pool is not None:
            self.pools[cur_pool].remove_user(user_id)
        await self._assign_user_to_pool(user_id, pool_type, best_pool)

    #########################################################################################################
    #####  Dynamic Pool Management
    #########################################################################################################

    async def _check_reassign_user(self, user_id: str, pool_type: PoolType) -> None:
        if self.users[user_id].get_active_tasks(pool_type) == 0:
            current_pool = self.users[user_id].get_pool(pool_type)
            best_pool: UUID = await self._get_least_utilized_pool_of_type(pool_type)

            if current_pool == None or best_pool != current_pool:
                await self._reassign_user(user_id, pool_type, best_pool)

    async def _assign_user_to_best_pool(self, user_id: str, pool_type: PoolType) -> None:
        pool_id: UUID = await self._get_least_utilized_pool_of_type(pool_type)
        await self._assign_user_to_pool(user_id, pool_type, pool_id)

    async def _ensure_pool(self, user_id: str, pool_type: PoolType) -> UUID:
        if not await self._user_is_registered(user_id):
            await self._register_user(user_id)
        else:
            pool_id: Optional[UUID] = self.users[user_id].get_pool(pool_type)
            if pool_id is None:
                # user is registered but not assigned to pool of this type
                await self._assign_user_to_best_pool(user_id, pool_type)
            else:
                await self._check_reassign_user(user_id, pool_type)

        pool_id = cast(UUID, self.users[user_id].get_pool(pool_type))
        return pool_id
    
    # Tiebreaker:
    # (1) Choose pool with most idle workers (prevent underutilization)
    # (2) Choose pool with least cost of active + pending tasks #TODO: update to which pool will have an idle worker first??
    # (3) Choose pool with least users
    # (4) Choose pool with least number of pending tasks
    # (5) Choose arbitrary 
    async def _get_least_utilized_pool_of_type(self, type: PoolType) -> UUID:
        pool_ids: List[UUID] = self.pool_of_type_mapping.get_all_pool_of_type(type)
        assert len(pool_ids) > 0
        least_utilized_pool_id: UUID = pool_ids[0]

        def current_pool_is_worse(current_pool_id:UUID, compare_pool_id: UUID) -> bool:
            current_pool: _PoolWrapper = self.pools[current_pool_id]
            compare_pool: _PoolWrapper = self.pools[compare_pool_id]

            current_pool_workers: int = current_pool.get_worker_count()
            compare_pool_workers: int = compare_pool.get_worker_count()

            current_pool_idle_workers: float = current_pool.get_idle_workers() / current_pool_workers
            compare_pool_idle_workers: float = compare_pool.get_idle_workers() / compare_pool_workers

            if compare_pool_idle_workers != current_pool_idle_workers: #higher percentage of idle workers
                return compare_pool_idle_workers > current_pool_idle_workers

            current_pool_utilization: float = current_pool.get_utilization() / current_pool_workers
            compare_pool_utilization: float = compare_pool.get_utilization() / compare_pool_workers
            
            if compare_pool_utilization != current_pool_utilization: #lower utilization 
                return compare_pool_utilization < current_pool_utilization
            
            current_pool_user_count: float = current_pool.get_user_count() / current_pool_workers
            compare_pool_user_count: float = compare_pool.get_user_count() / compare_pool_workers

            if compare_pool_user_count != current_pool_user_count: #lower user count
                return compare_pool_user_count < current_pool_user_count
            
            current_pool_pending_tasks: float = current_pool.get_pending_tasks_count() / current_pool_workers #since we know pool is not idle
            compare_pool_pending_tasks: float = compare_pool.get_pending_tasks_count() / compare_pool_workers

            if compare_pool_pending_tasks != current_pool_pending_tasks: #lower pending tasks
                return compare_pool_pending_tasks < current_pool_pending_tasks
            
            return compare_pool.get_pool_id() < current_pool.get_pool_id() #arbitrary tiebreaker

        for pool_id in pool_ids:
            if current_pool_is_worse(least_utilized_pool_id, pool_id):
                least_utilized_pool_id = pool_id
        
        return least_utilized_pool_id

    def _get_active_pool_types(self) -> List[PoolType]:
        return self.config.get_types()
    
    #########################################################################################################
    #####  Add Tasks and Batches
    #########################################################################################################
   
    async def add_task(self, task: PoolTask) -> Tuple[UUID, EnhancedFuture]:
        return (await self.add_batch_tasks_of_same_user_same_pool([task]))[0]
   
    async def add_batch_tasks_of_same_user_same_pool(self, tasks: List[PoolTask]) -> List[Tuple[UUID, EnhancedFuture]]:
        assert len(tasks) > 0 #TODO Check
        user_id: str = tasks[0].user_id
        pool_type: PoolType = tasks[0].pool_type

        pool_id: UUID = await self._ensure_pool(user_id, pool_type)
        pool: _PoolWrapper = self.pools[pool_id]

        for task in tasks:
            assert task.pool_type == pool_type
            assert task.user_id == user_id

            task.pool_id = pool_id
            task.user = self.users[user_id]

        task_ids_and_tuples: List[Tuple[UUID, EnhancedFuture]] = pool.add_batch_tasks_get_futures(tasks)
        return task_ids_and_tuples

    async def add_batch_tasks_same_user_different_pools(self, tasks: List[PoolTask]) -> List[Tuple[UUID, EnhancedFuture]]:
        assert len(tasks) > 0
        user_id: str = tasks[0].user_id
        unique_pools: Set[PoolType] = set([task.pool_type for task in tasks])
        pool_ids: Dict[PoolType, UUID] = {pool_type: await self._ensure_pool(user_id, pool_type) for pool_type in unique_pools}

        for pool_type in unique_pools:
            await self._check_reassign_user(user_id, pool_type)
        
        user: _ProcessPoolUser = self.users[user_id]
        ids_and_futures: List[Optional[Tuple[UUID, EnhancedFuture]]] = [None] * len(tasks)

        for pool_type in unique_pools:
            pool: _PoolWrapper = self.pools[pool_ids[pool_type]]
            batch_of_type: List[PoolTask] = []
            indicies_of_overall_batch: List[int]= []

            for i, task in enumerate(tasks):
                if task.pool_type == pool_type:
                    batch_of_type.append(task)
                    indicies_of_overall_batch.append(i)
            
            batch_futures_ands_tasks = pool.add_batch_tasks_get_futures(batch_of_type)
            for i, future in zip(indicies_of_overall_batch, batch_futures_ands_tasks):
                ids_and_futures[i] = future
            
            #TODO: assert EnhancedFutures has no none elements
            
        return cast(List[Tuple[UUID, EnhancedFuture]], ids_and_futures)
    
    #########################################################################################################
    #####  Get and Wait on Futures
    #########################################################################################################
    def _get_all_active_futures(self) -> List[EnhancedFuture]:
        all_futures: List[EnhancedFuture] = []
        for pool in self.pools.values():
            all_futures.extend([future for (_, future) in pool.get_all_pending_and_running_tasks()])
        return all_futures

    def _get_all_active_futures_of_user(self, user_id: str) -> List[EnhancedFuture]:
        all_futures: List[EnhancedFuture] = []
        for pool in self.pools.values():
            for task_id, future in pool.get_all_pending_and_running_tasks():
                if self.users[user_id].get_pool(pool.pool_type) == pool.get_pool_id():
                    all_futures.append(future)
        return all_futures

    async def wait_on_all_futures_of_user(self, user_id: str) -> None:
        all_futures: List[EnhancedFuture] = self._get_all_active_futures_of_user(user_id)
        await asyncio.gather(*[future for future in all_futures])

    async def wait_on_all_futures(self) -> None:
        all_futures: List[EnhancedFuture] = self._get_all_active_futures()
        await asyncio.gather(*[future for future in all_futures])

    
    #########################################################################################################
    #####  Shutdown
    #########################################################################################################
    async def terminate(self) -> None:
        await self.wait_on_all_futures()  # Ensure all tasks are registered and finished, could also do asyncio.sleep(0.1)
        for pool in self.pools.values():
            pool.terminate()

