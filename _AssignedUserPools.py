from typing import Optional
from uuid import UUID
from PoolType import PoolType


#########################################################################################################
#########################################################################################################
#####  _AssignedUSerPools (Private)                                                                 #####
##### - A class to keep track of which user is assigned to which pool.                              #####
#########################################################################################################
#########################################################################################################
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