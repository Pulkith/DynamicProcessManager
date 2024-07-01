import math
import time
from queue import Queue
from typing import Dict, ItemsView, Optional, Set
from uuid import UUID

from _PoolUtilization import _PoolUtilization
from PoolTask import PoolTask
from EnhancedFuture import EnhancedFuture
from _TaskStatus import _TaskStatus


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
        
