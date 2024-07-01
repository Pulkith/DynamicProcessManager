from _TaskStatus import _TaskStatus


    #########################################################################################################
    #########################################################################################################
    #####  _PoolUtilization (Private)
    #### - This class is used to mantain the pending and active cost utilization and workers of a pool
    #########################################################################################################
    #########################################################################################################
class _PoolUtilization:
    #########################################################################################################
    ##### Initialization
    #########################################################################################################

    def __init__(self, workers: int) -> None:

        self.pending_utilization: float = 0
        self.active_utilization: float = 0

        self.workers: int = workers
        self.active_workers: int = 0
    
    #########################################################################################################
    ##### Utilization Change
    #########################################################################################################

    def process_changed(self, cost, task_status: _TaskStatus) -> None:
        if task_status == _TaskStatus.PENDING:
            self.pending_utilization += cost
        elif task_status == _TaskStatus.RUNNING:
            self.pending_utilization -= cost
            self.active_utilization += cost
            self.active_workers += 1
        elif task_status == _TaskStatus.COMPLETED:
            self.active_utilization -= cost
            self.active_workers -= 1

    #########################################################################################################
    ##### Getters
    #########################################################################################################

    def get_pending_utilization(self) -> float:
        return self.pending_utilization
    
    def get_active_utilization(self) -> float:
        return self.active_utilization
    
    def get_utilization(self) -> float:
        return self.active_utilization + self.pending_utilization

    def get_active_workers(self) -> int:
        return self.active_workers

    def get_workers(self) -> int: #might be less than actual workers if pools are overinitialized
        return self.workers
    
    def idle_workers (self) -> int:
        return self.workers - self.active_workers
    
    def has_idle_workers(self) -> bool:
        return self.active_workers < self.workers


    #########################################################################################################
    ##### Setters
    #########################################################################################################
   
    def set_active_workers(self, workers: int) -> None:
        self.active_workers = workers
    

