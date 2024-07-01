import asyncio
from typing import Optional, Set
from uuid import UUID
from PoolType import PoolType
from _AssignedUserPools import _AssignedUserPools

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