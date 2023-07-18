import asyncio
import random
import time
from typing import Callable, Awaitable, Union

from configurations import TsunamiConfiguration


class TsunamiRunner:
    """Runner for tsunami benchmark."""

    configurations: tuple[TsunamiConfiguration]
    """ Configurations of the tasks to be run. """
    concurrency: int
    """ Number of maximum concurrent tasks. """
    total_count: int
    """ Total number of tasks to be run. """
    queueing_delay: Union[
        float, Callable[[int], float], Callable[[int], Awaitable[float]]
    ]
    """ Time to be waited before appending a new instance of the task to the execution queue. """
    task_delay: Union[float, Callable[[int], float], Callable[[int], Awaitable[float]]]
    """ Time to be waited after the task is selected from the queue and before it is actually executed. """

    _run_start_timestamp_ns: int
    """ Timestamp of the beginning of the run. """

    def __init__(
        self,
        *configurations: TsunamiConfiguration,
        concurrency: int = 1,
        total_count: int = 1,
        queueing_delay: Union[
            float, Callable[[int], float], Callable[[int], Awaitable[float]]
        ] = 0,
        task_delay: Union[
            float, Callable[[int], float], Callable[[int], Awaitable[float]]
        ] = 0,
    ):
        self.configurations = configurations
        self.concurrency = concurrency
        self.total_count = total_count
        self.queueing_delay = queueing_delay
        self.task_delay = task_delay
        self._run_start_timestamp_ns = -1

    async def _compute_delay(
        self,
        delay: Union[
            float, Callable[[int], float], Callable[[int], Awaitable[float]]
        ] = 0,
    ) -> float:
        """Compute the delay based on the time elapsed since the beginning of the run."""
        if isinstance(delay, float):
            return delay
        elif callable(delay):
            if self._run_start_timestamp_ns == -1:
                raise RuntimeError(
                    "The run has not started yet. You cannot use a callable delay function."
                )
            if asyncio.iscoroutinefunction(delay):
                return await delay(
                    time.perf_counter_ns() - self._run_start_timestamp_ns
                )
            else:
                return delay(time.perf_counter_ns() - self._run_start_timestamp_ns)
        return 0

    async def run(self):
        """Run the benchmark."""
        self._run_start_timestamp_ns = time.perf_counter_ns()
        semaphore: asyncio.Semaphore = asyncio.Semaphore(self.concurrency)

        async def run_with_semaphore(_task: Callable[[], Awaitable[bool]]):
            async with semaphore:
                await asyncio.sleep(await self._compute_delay(self.task_delay))
                await _task()

        tasks = []
        configurations: list[TsunamiConfiguration] = random.choices(
            self.configurations,
            [config.weight for config in self.configurations],
            k=self.total_count,
        )
        for i in range(self.total_count):
            task = asyncio.create_task(run_with_semaphore(configurations[i].task))
            tasks.append(task)
            await asyncio.sleep(await self._compute_delay(self.queueing_delay))

        await asyncio.gather(*tasks)
