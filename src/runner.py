import asyncio
import random
from typing import Callable, Awaitable

from configurations import TsunamiConfiguration


class TsunamiRunner:
    """Runner for tsunami benchmark."""

    configurations: tuple[TsunamiConfiguration]
    """ Configurations of the tasks to be run. """
    concurrency: int
    """ Number of maximum concurrent tasks. """
    total_count: int
    """ Total number of tasks to be run. """
    queueing_delay: float
    """ Time to be waited before appending a new instance of the task to the execution queue. """
    task_delay: float
    """ Time to be waited after the task is selected from the queue and before it is actually executed. """

    def __init__(
        self,
        *configurations: TsunamiConfiguration,
        concurrency: int = 1,
        total_count: int = 1,
        queueing_delay: float = 0,
        task_delay: float = 0,
    ):
        self.configurations = configurations
        self.concurrency = concurrency
        self.total_count = total_count
        self.queueing_delay = queueing_delay
        self.task_delay = task_delay

    async def run(self):
        """Run the benchmark."""
        semaphore: asyncio.Semaphore = asyncio.Semaphore(self.concurrency)

        async def run_with_semaphore(_task: Callable[[], Awaitable[bool]]):
            async with semaphore:
                await asyncio.sleep(self.task_delay)
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
            await asyncio.sleep(self.queueing_delay)

        await asyncio.gather(*tasks)
