from enum import Enum
from typing import Any, Dict, List
from PoolType import PoolType

#########################################################################################################
#########################################################################################################
#####  _DynamicPoolConfig (Public)
#### - This class is used to define the configuration of the pools 
#########################################################################################################
#########################################################################################################
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