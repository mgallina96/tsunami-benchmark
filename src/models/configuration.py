from abc import ABC, abstractmethod
from uuid import uuid4


class TsunamiTask(ABC):
    """Base class for all TsunamiTask classes."""

    @abstractmethod
    async def run(self) -> tuple[bool, dict]:
        """Body of the task. Should return a tuple containing a boolean indicating whether
        the task was successful and a dictionary containing additional information."""
        raise NotImplementedError()


class TsunamiConfiguration:
    """Base class for all TsunamiConfiguration classes."""

    task: TsunamiTask
    """The task to be executed."""
    weight: float
    """The weight of the task. Used to calculate the probability of the task being executed."""
    code: str
    """Unique identifier of the configuration."""

    def __init__(
        self,
        task: TsunamiTask,
        *,
        weight: float = 1,
        code: str = None,
    ):
        self.task = task
        self.weight = weight
        self.code = code or str(uuid4())
