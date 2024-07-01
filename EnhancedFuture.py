import asyncio
import time
from typing import Any, Optional

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