"""
* [ ] Setup ssh connection in pre_execute?
    * We just use asyncssh connection foreach req now!
"""

from dataclasses import dataclass
from typing import Any, Callable, Dict, Optional, Union

from flytekit import FlyteContextManager, PythonFunctionTask
from flytekit.configuration import SerializationSettings
from flytekit.extend import TaskPlugins
from flytekit.extend.backend.base_agent import AsyncAgentExecutorMixin
from flytekit.image_spec import ImageSpec


@dataclass
class Slurm(object):
    """Pass now.

    Compared with spark, please refer to https://api-docs.databricks.com/python/pyspark/latest/api/pyspark.SparkContext.html
    """

    srun_conf: Optional[Dict[str, str]] = None

    def __post_init__(self):
        if self.srun_conf is None:
            self.srun_conf = {}


class SlurmTask(AsyncAgentExecutorMixin, PythonFunctionTask[Slurm]):
    """
    Actual Plugin that transforms the local python code for execution within a slurm context...
    """

    _TASK_TYPE = "slurm"

    def __init__(
        self,
        task_config: Slurm,
        task_function: Callable,
        container_image: Optional[Union[str, ImageSpec]] = None,
        **kwargs,
    ):
        super(SlurmTask, self).__init__(
            task_config=task_config,
            task_type=self._TASK_TYPE,
            task_function=task_function,
            container_image=container_image,
            **kwargs,
        )

    def get_custom(self, settings: SerializationSettings) -> Dict[str, Any]:
        return {"srun_conf": self.task_config.srun_conf}

    def execute(self, **kwargs) -> Any:
        ctx = FlyteContextManager.current_context()
        if ctx.execution_state and ctx.execution_state.is_local_execution():
            return AsyncAgentExecutorMixin.execute(self, **kwargs)
        else:
            return PythonFunctionTask.execute(self, **kwargs)


TaskPlugins.register_pythontask_plugin(Slurm, SlurmTask)
