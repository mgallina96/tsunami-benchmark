import asyncio

import pytest

from tsunami_benchmark.models.configuration import TsunamiTask, TsunamiConfiguration
from tsunami_benchmark.runner import TsunamiRunner


START = "START"
STOP = "STOP"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "total_count, concurrency, expected",
    [
        (10, 1, ([START] * 1 + [STOP] * 1) * 10),
        (10, 2, ([START] * 2 + [STOP] * 2) * 5),
        (10, 3, ([START] * 3 + [STOP] * 3) * 3 + ([START] * 1 + [STOP] * 1) * 1),
        (10, 4, ([START] * 4 + [STOP] * 4) * 2 + ([START] * 2 + [STOP] * 2) * 1),
        (10, 5, ([START] * 5 + [STOP] * 5) * 2),
        (10, 6, ([START] * 6 + [STOP] * 6) * 1 + ([START] * 4 + [STOP] * 4) * 1),
        (10, 7, ([START] * 7 + [STOP] * 7) * 1 + ([START] * 3 + [STOP] * 3) * 1),
        (10, 8, ([START] * 8 + [STOP] * 8) * 1 + ([START] * 2 + [STOP] * 2) * 1),
        (10, 9, ([START] * 9 + [STOP] * 9) * 1 + ([START] * 1 + [STOP] * 1) * 1),
        (10, 10, ([START] * 10 + [STOP] * 10) * 1),
    ],
)
async def test_runner__concurrent_execution(
    total_count: int, concurrency: int, expected: list[str]
):
    """Test that the runner executes tasks concurrently."""
    output: list[str] = []

    class TestTask(TsunamiTask):
        async def run(self) -> tuple[bool, dict]:
            output.append(START)
            await asyncio.sleep(0.2)
            output.append(STOP)
            return True, {}

    configuration = TsunamiConfiguration(TestTask())
    runner = TsunamiRunner(
        configuration, total_count=total_count, concurrency=concurrency
    )

    _ = await runner.run()

    assert output == expected
