import asyncio
import random
import time
from datetime import datetime
from typing import Callable, Awaitable, Union

from models.configuration import TsunamiConfiguration
from models.result import TaskResult, BenchmarkResult, ConfigurationResult


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
    run_started_at: datetime
    """ Timestamp of the beginning of the run. """
    run_ended_at: datetime
    """ Timestamp of the end of the run. """

    _run_start_timestamp_ns: int
    """ Timestamp of the beginning of the run. Value with better precision 
    than 'run_started_at' used for measuring performance."""
    _run_end_timestamp_ns: int
    """ Timestamp of the end of the run. Value with better precision
    than 'run_ended_at' used for measuring performance."""

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
        self._run_end_timestamp_ns = -1

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

    async def run(self) -> BenchmarkResult:
        """Run the benchmark."""
        self._run_start_timestamp_ns = time.perf_counter_ns()
        self.run_started_at = datetime.now()
        semaphore: asyncio.Semaphore = asyncio.Semaphore(self.concurrency)
        configuration_results: dict[str, ConfigurationResult] = {
            c.code: ConfigurationResult(c, []) for c in self.configurations
        }

        async def run_with_semaphore(configuration: TsunamiConfiguration):
            async with semaphore:
                await asyncio.sleep(await self._compute_delay(self.task_delay))
                start_timestamp_ns = time.perf_counter_ns()
                try:
                    result = await configuration.task.run()
                except Exception as e:
                    result = (False, {"error": str(e)})
                end_timestamp_ns = time.perf_counter_ns()
                configuration_results[configuration.code].task_results.append(
                    TaskResult(
                        result,
                        start_timestamp_ns,
                        end_timestamp_ns,
                    )
                )

        tasks = []
        configurations: list[TsunamiConfiguration] = random.choices(
            self.configurations,
            [config.weight for config in self.configurations],
            k=self.total_count,
        )
        for i in range(self.total_count):
            task = asyncio.create_task(run_with_semaphore(configurations[i]))
            tasks.append(task)
            await asyncio.sleep(await self._compute_delay(self.queueing_delay))

        await asyncio.gather(*tasks)
        self._run_end_timestamp_ns = time.perf_counter_ns()
        self.run_ended_at = datetime.now()
        return BenchmarkResult(
            list(configuration_results.values()),
            self._run_start_timestamp_ns,
            self._run_end_timestamp_ns,
            self.run_started_at,
            self.run_ended_at,
        )
