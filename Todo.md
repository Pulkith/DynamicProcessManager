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
#TODO: Intefrate EnhancedFuture like cost, elasped_time, maintain other stuff?
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

