from typing import List
from uuid import UUID
from PoolType import PoolType

    #########################################################################################################
    #########################################################################################################
    #####  _PoolTypeMapping (Private)
    #### - This class is used to mantain all the pools of a specific type
    #########################################################################################################
    #########################################################################################################
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