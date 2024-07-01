from __future__ import annotations
import multiprocessing as mp
from concurrent.futures import ProcessPoolExecutor
import asyncio
import time 
from uuid import uuid4, UUID
from functools import partial
from typing import Any, Callable, List, Tuple, Dict, Optional, cast, Awaitable, Set, ItemsView
from enum import Enum
import contextlib
import math
from queue import Queue

class PoolType(Enum):
    HEAVY = 1 
    EXPRESS = 2

class _TaskStatus(Enum):
    PENDING = 1
    RUNNING = 2
    COMPLETED = 3

class PoolTask:
    def __init__(self, user_id: str, pool_type: PoolType, cost: Optional[float], func: Callable, callback: Optional[Callable], *args, **kwds) -> None:
        # self.task_id: UUID = task_id
        self.user_id: str = user_id
        self.pool_type: PoolType = pool_type
        self.user: Optional[_ProcessPoolUser] = None
        self.task_id: UUID = uuid4()
        self.pool_id: Optional[UUID] = None
        self.cost: Optional[float] = cost
        self.func: Callable = func
        self.callback: Optional[Callable] = callback
        self.args: Tuple = args
        self.kwds: Dict = kwds

        self.queue_time: float = time.time()
        self.start_time: Optional[float] = None
        self.end_time: Optional[float] = None
    
    def get_cost(self) -> float:
        return self.cost if self.cost is not None else 0

    def get_running_time(self) -> Optional[float]:
        if self.end_time is None:
            return None
        assert self.start_time is not None and self.end_time is not None
        return self.end_time - self.start_time

    def get_queue_time(self) -> float:
        return time.time() - self.queue_time

    def set_start_time(self) -> None:
        self.start_time = time.time()

    def set_end_time(self) -> None:
        self.end_time = time.time()

    def __str__(self) -> str:
        # return f"Task ID: {self.task_id} User ID: {self.user_id} Pool ID: {self.pool_id} Cost: {self.cost} Func: {self.func} Args: {self.args} Kwds: {self.kwds}"
        return ""

class _AssignedUserPools:
    def __init__(self) -> None:
        self.pools: dict[PoolType, UUID] = {}
    
    def assign_pool(self, pool_type: PoolType, pool_id: Optional[UUID]) -> None:
        if pool_id is None:
            if pool_type in self.pools:
                del self.pools[pool_type]
        else:
            self.pools[pool_type] = pool_id
    
    def get_pool(self, pool_type: PoolType) -> Optional[UUID]:
        if pool_type in self.pools:
            return self.pools[pool_type]
        else:
            return None

class _ProcessPoolUser:
    def __init__(self, user_id: str) -> None:
        self.assigned_pools: _AssignedUserPools = _AssignedUserPools()
        self.user_id: str = user_id
        self.active_tasks: dict[PoolType, Set[UUID]] = {} #TODO: seperate into running and pending tasks, also add finishedtimes

    def assign_pool(self, pool_type: PoolType, pool_id: UUID) -> None:
        self.assigned_pools.assign_pool(pool_type, pool_id)     
    
    def get_pool(self, pool_type: PoolType) -> Optional[UUID]:
        return self.assigned_pools.get_pool(pool_type)
    
    def un_assign_pool(self, pool_type: PoolType) -> None:
        self.assigned_pools.assign_pool(pool_type, None)
    
    def reassign_pool(self, pool_type: PoolType, pool_id: UUID) -> None:
        self.assigned_pools.assign_pool(pool_type, pool_id)

    def task_started(self, pool_type: PoolType, task_id: UUID) -> None:
        if pool_type in self.active_tasks:
            self.active_tasks[pool_type].add(task_id)
        else:
            self.active_tasks[pool_type] = {task_id}
    
    def task_completed(self, pool_type: PoolType, task_id: UUID) -> None:
        try:
            assert pool_type in self.active_tasks
            self.active_tasks[pool_type].remove(task_id)
            if len(self.active_tasks[pool_type]) == 0:
                del self.active_tasks[pool_type]
        except KeyError:
            print(f"Error: Task {task_id} not found in active tasks of user {self.user_id}")
    
    def get_active_tasks(self, pool_type) -> int:
        if pool_type in self.active_tasks:
            return len(self.active_tasks[pool_type])
        else:
            return 0
    
    def get_all_active_tasks(self) -> int:
        return sum([len(tasks) for tasks in self.active_tasks.values()])
    
    #TODO: add await on all tasks + check status/stall of all tasks of user
    def __str__(self) -> str:
        return f"User ID: {self.user_id} Active Tasks: {self.active_tasks}"

class _PoolUtilization:
    def __init__(self, workers: int) -> None:

        self.pending_utilization: float = 0
        self.active_utilization: float = 0

        self.workers: int = workers
        self.active_workers: int = 0
    

    def process_changed(self, cost, _TaskStatus) -> None:
        if _TaskStatus == _TaskStatus.PENDING:
            self.pending_utilization += cost
        elif _TaskStatus == _TaskStatus.RUNNING:
            self.pending_utilization -= cost
            self.active_utilization += cost
            self.active_workers += 1
        elif _TaskStatus == _TaskStatus.COMPLETED:
            self.active_utilization -= cost
            self.active_workers -= 1

    def get_pending_utilization(self) -> float:
        return self.pending_utilization
    
    def get_active_utilization(self) -> float:
        return self.active_utilization
    
    def get_utilization(self) -> float:
        return self.active_utilization + self.pending_utilization

    def set_active_workers(self, workers: int) -> None:
        self.active_workers = workers
    
    def get_active_workers(self) -> int:
        return self.active_workers

    def get_workers(self) -> int: #might be less than actual workers if pools are overinitialized
        return self.workers
    
    def idle_workers (self) -> int:
        return self.workers - self.active_workers
    
    def has_idle_workers(self) -> bool:
        return self.active_workers < self.workers

class _PoolTypeMapping:
    
    def __init__(self) -> None:

        self.mappings: dict[PoolType, List[UUID]] = {}

    def add_pool(self, pool_id: UUID, pool_type: PoolType) -> None:
        if pool_type in self.mappings:
            self.mappings[pool_type].append(pool_id)
        else:
            self.mappings[pool_type] = [pool_id]
    
    def get_all_pool_of_type(self, pool_type: PoolType) -> List[UUID]:
        if pool_type not in self.mappings:
            return []
        else:
            return self.mappings[pool_type]

class EnhancedFuture(asyncio.Future):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.cost = None
        self.start_time = time.time()
    
    def set_cost(self, cost: float) -> None:
        self.cost = cost
    
    def get_cost(self) -> Optional[float]:
        return self.cost
    
    def set_result(self, result: Any) -> None:
        elapsed_time = time.time() - self.start_time
        self.run_time = elapsed_time
        super().set_result(result)

class DynamicPoolConfig:
    def __init__(self) -> None:
        self.config: dict[PoolType, dict[str, Any]] = {}

    def set_config(self, pool_type: PoolType, num_pools_of_type: int, num_cores_per_pool: int):
        self.config[pool_type] = {}
        self.config[pool_type]['num_pools_of_type'] = num_pools_of_type
        self.config[pool_type]['num_cores_per_pool'] = num_cores_per_pool

    def get_config_of_type(self, pool_type: PoolType) -> Dict[str, Any]:
        return self.config[pool_type]
    
    def get_types(self) -> List[PoolType]:
        return list(self.config.keys())


    # Tiebreaker: SJF and WSPT Scheduling (minimizing AVERAGE wait time) -> tasks are ran in order of submission for each user: TODO: add priority later?
    # (1) Choose user with less currently running tasks (prevent monopolization of WORKERS)
    # (2) choose user with least cost of task (improve system throughput) + (3) Choose user with longest wait time within epsilon (prevent starvation) (based on herustic)
    # (4) Choose user with least amount of tasks in queue (prevent future monopolization of WORKERS)
    # (5) choose user who has been waiting longest since LAST task (TODO: implement fully, currently will just select person who did not finish last)
    # (6) Alphabetic order

class _PoolTaskQueueAndManager: # not actually a queue
    MAXIMIM_QUEUE_SIZE: int = -1
    EPSILON_HEURISTIC: float = 2.0

    def __init__(self, max_workers: int) -> None:
        self.pending_tasks: Dict[str, Queue[UUID]] = {} # user_id -> List[PoolTask], most effecient I can think of, List[] #TODO: convert to Queue
        self.running_tasks: Dict[str, Set[UUID]] = {} # user_id -> List[PoolTask]

        self.task_futures: Dict[UUID, EnhancedFuture] = {} # task_id -> EnhancedFuture
        self.tasks: Dict[UUID, PoolTask] = {} # task_id -> PoolTask

        self.pool_utilization: _PoolUtilization = _PoolUtilization(max_workers)
        self.users: Set[str] = set()

    def _wait_cost_heuristic(self, wait_time: float, wait_max: float, cost_of_task: float, cost_max: float) -> float:
        # change to exponential increase after waiting for more than half the cost, otherwise linear, all in one equation
        # assume cost is in seconds to run task
        #UNIT_OF_TIME = 10 # 10 SECONDS
        #return (cost_of_task / UNIT_OF_TIME) + (wait_time // UNIT_OF_TIME + 1) * ((1.05) ** (wait_time / UNIT_OF_TIME))
        
        # example ranking
        # wait_time, cost
        # 1000, 50
        # 200, 10
        # 300, 40
        # 1500, 60
        # 800, 20
        COST_WEIGHT: float = 0.5
        WAIT_WEIGHT: float  = 0.5
        wait_score: float = WAIT_WEIGHT * (math.exp(wait_time / wait_max)) 
        cost_score: float = COST_WEIGHT * math.log10(cost_max / (cost_of_task + math.pow(10, -2)) + 1)
        score: float = wait_score + cost_score
        return score
    
    def _get_worker_usage(self, user_id: str) -> int:
        return len(self.running_tasks[user_id]) if user_id in self.running_tasks else 0

    def _get_running_cost_of_user(self, user_id: str) -> float:
        sum = 0
        if user_id in self.running_tasks:
            for task_id in self.running_tasks[user_id]:
                if self.tasks[task_id].cost is not None:
                    sum += self.tasks[task_id].get_cost()
        return sum
    
    def _get_longest_wait_time(self, user_id: str, compare_time: float = time.time()) -> float:
        return compare_time - self.tasks[self.pending_tasks[user_id].queue[0]].queue_time

    def _get_highest_cost_task_of_user(self, user_id: str) -> float:
        if user_id not in self.pending_tasks:
            return 0
        max_cost: float = 0
        for task_id in list(self.pending_tasks[user_id].queue):
            if self.tasks[task_id].cost is not None and self.tasks[task_id].get_cost() > max_cost:
                max_cost = self.tasks[task_id].get_cost()
        return max_cost
    
    def pop_next_task(self, last_finisher: Optional[str] = None) -> Optional[UUID]:
        if len(self.pending_tasks) == 0:
            return None
        
        def is_current_worse(compare: str, current_best: str) -> bool:
            current_worker_usage: float = self._get_worker_usage(current_best)
            compare_worker_usage: float = self._get_worker_usage(compare)

            if compare_worker_usage != current_worker_usage:
                return compare_worker_usage < current_worker_usage
        
            highest_wait_time_across_all_users: float = max([self._get_longest_wait_time(user) for user in self.pending_tasks.keys()])
            highest_task_cost_across_all_users: float = max([self._get_highest_cost_task_of_user(user) for user in self.pending_tasks.keys()])

            current_user_cost: float = 0
            if self.tasks[self.pending_tasks[current_best].queue[0]].get_cost() is not None:
                current_user_cost = self.tasks[self.pending_tasks[current_best].queue[0]].get_cost()
            heuristic_score_current: float = self._wait_cost_heuristic(self._get_longest_wait_time(current_best), highest_wait_time_across_all_users, current_user_cost, highest_task_cost_across_all_users)

            compare_user_cost: float = 0
            if self.tasks[self.pending_tasks[compare].queue[0]].get_cost() is not None:
                compare_user_cost = self.tasks[self.pending_tasks[compare].queue[0]].get_cost()
            
            heuristic_score_compare: float = self._wait_cost_heuristic(self._get_longest_wait_time(compare), highest_wait_time_across_all_users, compare_user_cost, highest_task_cost_across_all_users)

            if abs(heuristic_score_compare - heuristic_score_current) < _PoolTaskQueueAndManager.EPSILON_HEURISTIC:
                return heuristic_score_compare > heuristic_score_current
            
            if self.pending_tasks[compare].qsize() != self.pending_tasks[current_best].qsize():
                return self.pending_tasks[compare].qsize() < self.pending_tasks[current_best].qsize()
            
            if last_finisher is not None:
                if last_finisher == current_best:
                    return True
                elif last_finisher == compare:
                    return False
            
            return compare < current_best #arbitrary tiebreaker
            
        # find user whose task should go next next

        user_to_retrieve_task: str = list(self.pending_tasks.keys())[0] #placeholder
        for user_id, _ in self.pending_tasks.items():
            if is_current_worse(user_id, user_to_retrieve_task):
                user_to_retrieve_task = user_id
        
        next_task: PoolTask = self.tasks[self.pending_tasks[user_to_retrieve_task].get_nowait()]
        if self.pending_tasks[user_to_retrieve_task].qsize() == 0:
            del self.pending_tasks[user_to_retrieve_task]
        
        return next_task.task_id
    
    def add_task(self, task: PoolTask) -> None:
        self.tasks[task.task_id] = task
        if task.user_id not in self.pending_tasks:
            self.pending_tasks[task.user_id] = Queue(maxsize=0)
        self.pending_tasks[task.user_id].put_nowait(task.task_id)
        self.pool_utilization.process_changed(task.get_cost(), _TaskStatus.PENDING)
        
    def has_idle_workers(self) -> bool:
        return self.pool_utilization.has_idle_workers()

    def has_pending_tasks(self) -> bool:
        return len(self.pending_tasks) > 0

    def set_task_running(self, task_id: UUID) -> None:
        task: PoolTask = self.tasks[task_id]
        user_id: str = task.user_id

        if user_id not in self.running_tasks:
            self.running_tasks[user_id] = set()
        
        task.set_start_time()
        
        self.running_tasks[user_id].add(task.task_id)
        self.pool_utilization.process_changed(task.get_cost(), _TaskStatus.RUNNING)
    
    def set_task_finished(self, task_id) -> None:
        task_cost = self.tasks[task_id].get_cost()
        user_id: str = self.tasks[task_id].user_id
        self.tasks[task_id].set_end_time() #TODO: use elasped time?
        del self.tasks[task_id]
        # DO NOT DELTE TASK_FUTURE TO PREVENT RACE CONDITION #TODO: delete task future after 10s after it has been retrieved

        self.running_tasks[user_id].remove(task_id)

        if len(self.running_tasks[user_id]) == 0:
            del self.running_tasks[user_id]
        self.pool_utilization.process_changed(task_cost, _TaskStatus.COMPLETED)

    def add_user(self, user_id: str) -> None:
        self.users.add(user_id)
    
    def remove_user(self, user_id: str) -> None:
        if user_id in self.pending_tasks or user_id in self.running_tasks:
            raise ValueError(f"Error Removing User From Pool: User {user_id} has pending or running tasks")
        if user_id in self.users:
            self.users.remove(user_id)

    def get_user_count (self) -> int:
        return len(self.users)

    def set_task_future(self, task_id: UUID, future: EnhancedFuture) -> None:
        self.task_futures[task_id] = future
    
    def get_task_future(self, task_id: UUID) -> EnhancedFuture:
        return self.task_futures[task_id]

    def get_all_futures(self) -> ItemsView[UUID, EnhancedFuture]:
        return self.task_futures.items()
    
    def get_utilization(self) -> float:
        return self.pool_utilization.get_utilization()
    
    def get_pending_utilization(self) -> float:
        return self.pool_utilization.get_pending_utilization()

    def get_active_utilization(self) -> float:
        return self.pool_utilization.get_active_utilization()
    
    def get_idle_workers(self) -> int:
        return self.pool_utilization.idle_workers()

    def get_pending_task_count(self) -> int:
        return sum([self.pending_tasks[user_id].qsize() for user_id in self.pending_tasks.keys()])
    
    def get_active_task_count(self) -> int:
        return sum([len(self.running_tasks[user_id]) for user_id in self.running_tasks.keys()])
    
    def get_task_count(self):
        return self.get_pending_task_count() + self.get_active_task_count()
    
    def get_worker_count(self) -> int:
        return self.pool_utilization.get_workers()
        

#########################################################################################################
#########################################################################################################
#####  Pool Wrapper                                                                                 #####
#########################################################################################################
#########################################################################################################
class _PoolWrapper():
    def __init__(self, pool_type: PoolType, num_workers: int) -> None:
        self.pool: ProcessPoolExecutor = ProcessPoolExecutor(max_workers=num_workers)
        self.pool_id: UUID = uuid4() 
        self.queue_manager: _PoolTaskQueueAndManager = _PoolTaskQueueAndManager(num_workers)

        self.pool_type: PoolType = pool_type
        self.event_loop: asyncio.AbstractEventLoop = asyncio.get_event_loop()

    def get_pool_id(self) -> UUID:
        return self.pool_id
    
    #########################################################################################################
    #####  User Management                                                                              #####
    #########################################################################################################
    def add_user(self, user_id: str) -> None:
        self.queue_manager.add_user(user_id)

    def remove_user(self, user_id: str) -> None:
        self.queue_manager.remove_user(user_id)

    def get_user_count(self) -> int:
        return self.queue_manager.get_user_count()
    #########################################################################################################
    #####  Utilization Management                                                                       #####
    #########################################################################################################
    # def get_utilization(self) -> float:
    #     return self.queue_manager.pool_utilization.get_utilization()
    
    #########################################################################################################
    #####  Task Completion                                                                              #####
    #########################################################################################################
    def shared_callback(self, future: EnhancedFuture, task_id: UUID) -> None:
        if future.exception():
            print(f"Task {task_id} failed with error: {future.exception}")
        
        self._try_start_new_task()
   
    #########################################################################################################
    #####  Task Run Logic                                                                               #####
    #########################################################################################################
    async def run_sync_task(self, func: Callable, *args, **kwds) -> Any:
        try:
            return await self.event_loop.run_in_executor(self.pool, partial(func, *args, **kwds))
        except Exception as e:
            # logger.error(f"Error running sync task: {e}")
            raise

    def _run_async_in_executor(self, func: Callable, *args, **kwds) -> Any:
        loop: asyncio.AbstractEventLoop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            coro: Awaitable= func(*args, **kwds)
            if not asyncio.iscoroutine(coro):
                raise TypeError("The provided function is not an async function")
            result: Any = loop.run_until_complete(coro)
        except Exception as e:
            print(f"Error Running Async: {e}")
            return None
        finally:
            loop.close()
        return result
    
    async def run_async_task(self, func: Callable, *args, **kwds) -> Any:
        return await self.event_loop.run_in_executor(self.pool, 
                                                     partial(self._run_async_in_executor, 
                                                             func, *args, **kwds))
    
    def _try_start_new_task(self) -> None:
        if not self.queue_manager.has_idle_workers() or not self.queue_manager.has_pending_tasks():
            return
        
        next_task_id: Optional[UUID] = self.queue_manager.pop_next_task()
        if next_task_id is None:
            return
        assert next_task_id is not None
        self.queue_manager.set_task_running(next_task_id)

        task: PoolTask = self.queue_manager.tasks[next_task_id]
        future: EnhancedFuture = self.queue_manager.get_task_future(next_task_id)

        async def _task_wrapper() -> None:
            try: 
                if asyncio.iscoroutinefunction(task.func):
                    result: Any = await self.run_async_task(task.func, *task.args, **task.kwds)
                else:
                    result: Any = await self.run_sync_task(task.func, *task.args, **task.kwds)
                future.set_result(result)
            except Exception as e:
                future.set_exception(e)
        
        def combined_callback(future: EnhancedFuture) -> None:
            assert task.user is not None
            result: Any = future.result() if not future.exception() else future.exception()

            task.user.task_completed(self.pool_type, task.task_id)
            self.queue_manager.set_task_finished(task.task_id)

            self.shared_callback(future, task.task_id)
            if task.callback is not None:
                task.callback(result, task.task_id)

        future.add_done_callback(combined_callback)
        asyncio.ensure_future(_task_wrapper())

    def _add_task(self, task: PoolTask) -> UUID:
        assert task.user is not None

        if task.cost is None:
            task.cost = self.get_task_cost()
        
        self.queue_manager.add_task(task)

        future = EnhancedFuture()
        self.queue_manager.set_task_future(task.task_id, future)
        # future.set_cost(task.cost)

        task.user.task_started(self.pool_type, task.task_id)
        self._try_start_new_task()
        
        return task.task_id  #will likely return before task is even added to pool due to run_in_executor
    
    #########################################################################################################
    #####  Add Tasks & Batches to Pool Helpers                                                          #####
    #TODO: Add _add_batch_tasks method / chunksize: might not be possible because no map function in asyncio and might not help since tasks are variable length
    #########################################################################################################
    async def add_task_and_wait(self, task: PoolTask) -> Any:
        task_id: UUID = self._add_task(task)
        result: Any = await self.get_task_result(task_id)
        return result
    
    def add_task_get_future(self, task: PoolTask) -> Tuple[UUID, EnhancedFuture]:
        task_id: UUID = self._add_task(task)
        return (task_id, self.queue_manager.get_task_future(task_id))

    async def add_batch_tasks_and_wait(self, tasks: List[PoolTask]) -> List[Any]:
        task_ids: List[UUID] = [self._add_task(task) for task in tasks]
        results: List[Any] = await asyncio.gather(*[self.get_task_result(task_id) for task_id in task_ids])
        return results
    
    def add_batch_tasks_get_futures(self, tasks: List[PoolTask]) -> List[Tuple[UUID, EnhancedFuture]]:
        task_ids: List[UUID] = [self._add_task(task) for task in tasks]
        return [(task_id, self.queue_manager.get_task_future(task_id) )for task_id in task_ids]

    async def get_task_result(self, task_id: UUID) -> Any:
        if task_id not in self.queue_manager.task_futures:
            raise ValueError(f"Task {task_id} not found in pool {self.pool_id}")
        return await self.queue_manager.get_task_future(task_id)

    def get_all_pending_and_running_tasks(self) -> ItemsView[UUID, EnhancedFuture]:
        return self.queue_manager.get_all_futures()

    def get_utilization(self) -> float:
        return self.queue_manager.get_utilization()

    def get_pending_utilization(self) -> float:
        return self.queue_manager.get_pending_utilization()
    
    def get_active_utilization(self) -> float:
        return self.queue_manager.get_active_utilization()
    
    def get_idle_workers(self) -> int:
        return self.queue_manager.pool_utilization.idle_workers()

    def get_pending_tasks_count(self) -> int:
        return self.queue_manager.get_pending_task_count()
    
    def get_active_tasks_count(self) -> int:
        return self.queue_manager.get_active_task_count()
    
    def get_task_count(self) -> int:
        return self.queue_manager.get_task_count()
    
    def get_worker_count(self) -> int:
        return self.queue_manager.get_worker_count()


    #########################################################################################################
    #####  Determine Task Cost                                                                      #####
    #########################################################################################################
    def get_task_cost(self) -> float:
        #TODO Implement
        return 1.0
     

    #########################################################################################################
    #####  Shutdown Pool MEthod                                                                          #####
    #########################################################################################################
    def terminate(self) -> None:
        self.pool.shutdown(wait=True)
    
    def __str__(self) -> str:
        return f"Pool ID: {self.pool_id}"

class _DynamicProcessPoolStateInstance:

    def __init__(self, config: DynamicPoolConfig) -> None:
        self.config: DynamicPoolConfig = config

        self.pools: dict[UUID, _PoolWrapper] = {}
        self.pool_of_type_mapping: _PoolTypeMapping = _PoolTypeMapping()

        self.users: dict[str, _ProcessPoolUser] = {}

        self._create_pools()

    def _register_pool(self, type: PoolType, cores: int) -> None:
        pool: _PoolWrapper = _PoolWrapper(type, cores)
        pool_id: UUID = pool.get_pool_id()
        self.pools[pool_id] = pool
        self.pool_of_type_mapping.add_pool(pool_id, type)

    def _get_active_pool_types(self) -> List[PoolType]:
        return self.config.get_types()
    
    def _create_pools(self) -> None:
        pool_types: List[PoolType]= self.config.get_types()
        for pool_type in pool_types:
            num_pools_of_type: int = self.config.get_config_of_type(pool_type)['num_pools_of_type']
            num_cores_per_pool: int= self.config.get_config_of_type(pool_type)['num_cores_per_pool']
            for _ in range(num_pools_of_type):
                self._register_pool(pool_type, num_cores_per_pool)

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
    
    #TODO: helper to add batch different POOL types AND different Pool Users??
    #TODO: add tasks as a batch (combined larger task)
    
    async def _assign_user_to_pool(self, user_id: str, pool_type: PoolType, pool_id: UUID) -> None:
        self.users[user_id].reassign_pool(pool_type, pool_id)
        self.pools[pool_id].add_user(user_id)

    async def _assign_user_to_best_pool(self, user_id: str, pool_type: PoolType) -> None:
        pool_id: UUID = await self._get_least_utilized_pool_of_type(pool_type)
        await self._assign_user_to_pool(user_id, pool_type, pool_id)

    async def _register_user(self, user_id: str) -> None:
        self.users[user_id] = _ProcessPoolUser(user_id)
        for type in self._get_active_pool_types():
            await self._assign_user_to_best_pool(user_id, type)

    async def _user_is_registered(self, user_id: str) -> bool:
        return (user_id in self.users and self.users[user_id] is not None)
    
    async def _check_reassign_user(self, user_id: str, pool_type: PoolType) -> None:
        if self.users[user_id].get_active_tasks(pool_type) == 0:
            current_pool = self.users[user_id].get_pool(pool_type)
            best_pool: UUID = await self._get_least_utilized_pool_of_type(pool_type)

            if current_pool == None or best_pool != current_pool:
                await self._reassign_user(user_id, pool_type, best_pool)

    async def _reassign_user(self, user_id: str, pool_type: PoolType, best_pool: UUID) -> None:
        cur_pool: Optional[UUID] = self.users[user_id].get_pool(pool_type)
        if cur_pool is not None:
            self.pools[cur_pool].remove_user(user_id)
        await self._assign_user_to_pool(user_id, pool_type, best_pool)

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

    async def terminate(self) -> None:
        await self.wait_on_all_futures()  # Ensure all tasks are registered and finished, could also do asyncio.sleep(0.1)
        for pool in self.pools.values():
            pool.terminate()

class DynamicProcessPool:
    def __init__(self, config: DynamicPoolConfig) -> None:
        self._config = config
        self.pool_manager: _DynamicProcessPoolStateInstance = _DynamicProcessPoolStateInstance(self._config)
   
    async def add_task(self, task: PoolTask) -> Tuple[UUID, EnhancedFuture]:
        return await self.pool_manager.add_task(task)
    
    async def add_task_wait(self, task: PoolTask) -> Any: 
        task_id_and_future: Tuple[UUID, EnhancedFuture] = await self.add_task(task)
        result: Any = await task_id_and_future[1]
        return result
    
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
        
    async def close_all_pools(self) -> None:
        await self.pool_manager.terminate()
    
    async def wait_on_all_futures_of_user(self, user_id: str) -> None:
        await self.pool_manager.wait_on_all_futures_of_user(user_id)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        asyncio.run(self.close_all_pools()) #not best practcice?

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
    manager: DynamicProcessPool = DynamicProcessPool(config=config)
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


#TODO: everytime a task is finished across any pool
# , if a user is still waiting for his tasks, check if he can be reassigned to a better pool
#TODO: When a task in a pool finishes, execute whoevers is next in line that has been waiting the longest. 
#TODO: incorporate cost of task into pool utilization
#TODO: incorproate task priority?
#TODO: asyncio.lock?
#TODO: use libraru for utilization instead of cost or combine both?
#TODO: thread safety

#TODO: FINISH Comments


#TODO: change least-utiloized pool to be based on which pool has most idle workers -> which pool has an empty slot first -> which pool has the least amount of tasks running
#TODO: add pools of same type but different number of cores
#TODO: for pool utilization, when choosing pool for first time, if user added a batch of tasks, take into account total sum of costs of tasks
#TODO: for each pool calcluate the time the first task will finish (or ) the time a new user's task will start

# TODO: fix calling with async functions not working

#look at mp.queue, synch primitiave,s pipes

#TODO:: Semaphore


# await asyncio.wait_for(eternity(), timeout=1.0)




#TODO update utilization to get pool with most idle workers -> lowest cost -> least time until user taks can be run


#TODO Make everything async


  # async def run_async_task(self, func: Callable, *args, **kwds) -> Any:
    #     #TODO check asyncio.run vs asyncio.get_event_loop().run_until_complete vs to_threadas
    #     # vs create_task vs 
    #     p_func = partial(asyncio.run)

    #     async def run_async_task(self, func: Callable, *args, **kwargs) -> Any:
    #     loop = asyncio.get_running_loop()
    #     with contextlib.suppress(asyncio.CancelledError):
    #         result_future = loop.create_future()
    #         await loop.run_in_executor(self.executor, self._run_in_subprocess, func, result_future, *args, **kwargs)
    #         return await result_future

    # def _run_in_subprocess(self, func: Callable, future: asyncio.Future, *args, **kwargs):
    #     asyncio.run(self._run_and_set_future(func, future, *args, **kwargs))

    # async def _run_and_set_future(self, func: Callable, future: asyncio.Future, *args, **kwargs):
    #     try:
    #         result = await func(*args, **kwargs)
    #         future.set_result(result)
    #     except Exception as e:
    #         future.set_exception(e)


    # def full_heuristic(self, user_id: str) -> None:
    #     pass
    # score = (a) * ((W + e) / T) + B * (1 / U) + G * log(1 + R) * g * P - n * L
    # S = score
    # W = wait time
    # e = epsilon
    # T = task cost
    # U = how many tasks user has running
    # R = how computaitionally expensive the task is 
    # P = priority of task
    # L number of tasks in queue for user
    # e = Epsiolon so tasks with very small cost are not starved
    # a, B, G, g, n = weights
    # example weights: a = 0.4 b = 0.2, G = 0.1 , g = 0.2, n = 0.1

    # make into PIP package + module