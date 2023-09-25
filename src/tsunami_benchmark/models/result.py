from datetime import datetime

from tsunami_benchmark.models.configuration import TsunamiConfiguration


class TaskResult:
    success: bool
    """Whether the configuration was executed successfully."""
    additional_info: dict
    """Additional information about the task execution."""
    start_timestamp_ns: int
    """Timestamp of the beginning of the task execution."""
    end_timestamp_ns: int
    """Timestamp of the end of the task execution."""

    def __init__(
        self, result: tuple[bool, dict], start_timestamp_ns: int, end_timestamp_ns: int
    ):
        self.success, self.additional_info = result
        self.start_timestamp_ns = start_timestamp_ns
        self.end_timestamp_ns = end_timestamp_ns

    @property
    def duration(self) -> float:
        """Duration of the task execution in seconds."""
        return round((self.end_timestamp_ns - self.start_timestamp_ns) / 1e9, 3)


class ConfigurationResult:
    """Result of a configuration run."""

    configuration: TsunamiConfiguration
    """The configuration that was executed."""
    task_results: list[TaskResult]
    """List of results for each task run."""

    def __init__(
        self, configuration: TsunamiConfiguration, task_results: list[TaskResult]
    ):
        self.configuration = configuration
        self.task_results = task_results

    @property
    def total_count(self) -> int:
        """Total number of tasks."""
        return len(self.task_results)

    @property
    def failures_count(self) -> int:
        """Number of failed tasks."""
        return sum(map(lambda task_result: not task_result.success, self.task_results))

    @property
    def successes_count(self) -> int:
        """Number of successful tasks."""
        return self.total_count - self.failures_count

    @property
    def failure_rate(self) -> float:
        """Failure rate of the configuration."""
        return round(
            self.failures_count / self.total_count if self.total_count > 0 else 0, 3
        )

    @property
    def success_rate(self) -> float:
        """Success rate of the configuration."""
        return round(1 - self.failure_rate, 3)

    @property
    def average_successful_duration(self) -> float:
        """Average duration of successful tasks."""
        return round(
            (
                sum(
                    map(
                        lambda task_result: task_result.duration,
                        filter(
                            lambda task_result: task_result.success, self.task_results
                        ),
                    )
                )
                / (self.total_count - self.failures_count)
                if (self.total_count - self.failures_count) > 0
                else 0
            ),
            3,
        )

    @property
    def average_failed_duration(self) -> float:
        """Average duration of failed tasks."""
        return round(
            (
                sum(
                    map(
                        lambda task_result: task_result.duration,
                        filter(
                            lambda task_result: not task_result.success,
                            self.task_results,
                        ),
                    )
                )
                / self.failures_count
                if self.failures_count > 0
                else 0
            ),
            3,
        )


class BenchmarkResult:
    """Result of a benchmark run."""

    configuration_results: list[ConfigurationResult]
    """List of results for each configuration run."""
    start_timestamp_ns: int
    """ Timestamp of the beginning of the run. Value with better precision 
    than 'run_started_at' used for measuring performance."""
    end_timestamp_ns: int
    """ Timestamp of the end of the run. Value with better precision
    than 'run_ended_at' used for measuring performance."""
    run_started_at: datetime
    """Timestamp of the beginning of the benchmark run."""
    run_ended_at: datetime
    """Timestamp of the end of the benchmark run."""

    def __init__(
        self,
        configuration_results: list[ConfigurationResult],
        run_start_timestamp_ns: int,
        end_timestamp_ns: int,
        run_started_at: datetime,
        run_ended_at: datetime,
    ):
        self.configuration_results = configuration_results
        self.start_timestamp_ns = run_start_timestamp_ns
        self.end_timestamp_ns = end_timestamp_ns
        self.run_started_at = run_started_at
        self.run_ended_at = run_ended_at

    @property
    def total_count(self) -> int:
        """Total number of tasks."""
        return sum(
            map(
                lambda configuration_result: configuration_result.total_count,
                self.configuration_results,
            )
        )

    @property
    def total_duration(self) -> float:
        """Total duration of the benchmark run in seconds."""
        return round((self.end_timestamp_ns - self.start_timestamp_ns) / 1e9, 3)

    def __str__(self):
        result = "BenchmarkResult\n"
        result += f"\tRun start timestamp: {self.run_started_at}\n"
        result += f"\tRun end timestamp: {self.run_ended_at}\n"
        result += f"\tTotal duration: {self.total_duration}s\n"
        result += f"\tTotal count: {self.total_count}\n"
        for configuration_result in self.configuration_results:
            result += f"\t{configuration_result.configuration.code}\n"
            result += f"\t\tTotal count: {configuration_result.total_count}\n"
            result += f"\t\tFailures: {configuration_result.failures_count}\n"
            result += f"\t\tSuccess rate: {configuration_result.success_rate}\n"
            result += f"\t\tAverage successful duration: {configuration_result.average_successful_duration}s\n"
            result += f"\t\tAverage failed duration: {configuration_result.average_failed_duration}s\n"
        return result
