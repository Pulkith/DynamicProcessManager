import asyncio
import time
from typing import Any, Optional

#########################################################################################################
#########################################################################################################
#####  EnhancedFuture (Public)
#### -  This class is used to extend the asyncio.Future class to include additional data (cost, elasped_time) <- Not Needed
#########################################################################################################
#########################################################################################################
class EnhancedFuture(asyncio.Future):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.cost: Optional[float] = None
        self.start_time: float = time.time()
        self.elasped_time: float = -1
    
    def set_cost(self, cost: float) -> None:
        self.cost = cost
    
    def get_cost(self) -> Optional[float]:
        return self.cost
    
    def set_result(self, result: Any) -> None:
        self.elapsed_time = time.time() - self.start_time
        super().set_result(result)