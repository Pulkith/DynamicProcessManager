
from PoolType import PoolType
from typing import Callable, Dict, Optional, Tuple
from uuid import UUID, uuid4
import time
from _ProcessPoolUser import _ProcessPoolUser

#########################################################################################################
#########################################################################################################
#####  PoolTask (Public)
#### -  This class is used to define a task that will be executed by the dynamic process pool
#########################################################################################################
#########################################################################################################
class PoolTask:
    
    #########################################################################################################
    #####  Initialization
    #########################################################################################################
    #TODO: change to setters/getters

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
    
    #########################################################################################################
    #####  Cost Default Getter (for Casting)
    #########################################################################################################
    
    def get_cost(self) -> float:
        return self.cost if self.cost is not None else 0

    #########################################################################################################
    #####  Timers
    #########################################################################################################
    
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


    #########################################################################################################
    #####  To String
    #########################################################################################################
    
    def __str__(self) -> str:
        # return f"Task ID: {self.task_id} User ID: {self.user_id} Pool ID: {self.pool_id} Cost: {self.cost} Func: {self.func} Args: {self.args} Kwds: {self.kwds}"
        return ""
