"""
* [ ] Setup ssh connection in pre_execute?
    * We just use asyncssh connection foreach req now!
"""

from dataclasses import dataclass
from typing import Any, Callable, Dict, Optional, Union

from flytekit import PythonFunctionTask
from flytekit.configuration import SerializationSettings
from flytekit.extend import TaskPlugins
from flytekit.extend.backend.base_agent import AsyncAgentExecutorMixin
from flytekit.image_spec import ImageSpec


@dataclass
class Slurm(object):
    """Pass now.

    Compared with spark, please refer to https://api-docs.databricks.com/python/pyspark/latest/api/pyspark.SparkContext.html
    """

    sbatch_conf: Optional[Dict[str, str]] = None

    def __post_init__(self):
        if self.sbatch_conf is None:
            self.sbatch_conf = {}


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
        return {"sbatch_conf": self.task_config.sbatch_conf}

    #     job = SparkJob(
    #         spark_conf=self.task_config.spark_conf,
    #         hadoop_conf=self.task_config.hadoop_conf,
    #         application_file=self._default_applications_path or "local://" + settings.entrypoint_settings.path,
    #         executor_path=self._default_executor_path or settings.python_interpreter,
    #         main_class="",
    #         spark_type=SparkType.PYTHON,
    #     )
    #     if isinstance(self.task_config, (Databricks, DatabricksV2)):
    #         cfg = cast(DatabricksV2, self.task_config)
    #         job._databricks_conf = cfg.databricks_conf
    #         job._databricks_instance = cfg.databricks_instance

    #     return MessageToDict(job.to_flyte_idl())

    # def pre_execute(self, user_params: ExecutionParameters) -> ExecutionParameters:
    #     import pyspark as _pyspark

    #     ctx = FlyteContextManager.current_context()
    #     sess_builder = _pyspark.sql.SparkSession.builder.appName(f"FlyteSpark: {user_params.execution_id}")
    #     if not (ctx.execution_state and ctx.execution_state.mode == ExecutionState.Mode.TASK_EXECUTION):
    #         # If either of above cases is not true, then we are in local execution of this task
    #         # Add system spark-conf for local/notebook based execution.
    #         spark_conf = _pyspark.SparkConf()
    #         spark_conf.set("spark.driver.bindAddress", "127.0.0.1")
    #         for k, v in self.task_config.spark_conf.items():
    #             spark_conf.set(k, v)
    #         # In local execution, propagate PYTHONPATH to executors too. This makes the spark
    #         # execution hermetic to the execution environment. For example, it allows running
    #         # Spark applications using Bazel, without major changes.
    #         if "PYTHONPATH" in os.environ:
    #             spark_conf.setExecutorEnv("PYTHONPATH", os.environ["PYTHONPATH"])
    #         sess_builder = sess_builder.config(conf=spark_conf)

    #     self.sess = sess_builder.getOrCreate()

    #     if (
    #         ctx.serialization_settings
    #         and ctx.serialization_settings.fast_serialization_settings
    #         and ctx.serialization_settings.fast_serialization_settings.enabled
    #         and ctx.execution_state
    #         and ctx.execution_state.mode == ExecutionState.Mode.TASK_EXECUTION
    #     ):
    #         file_name = "flyte_wf"
    #         file_format = "zip"
    #         shutil.make_archive(file_name, file_format, os.getcwd())
    #         self.sess.sparkContext.addPyFile(f"{file_name}.{file_format}")

    #     return user_params.builder().add_attr("SPARK_SESSION", self.sess).build()

    # def execute(self, **kwargs) -> Any:
    #     if isinstance(self.task_config, (Databricks, DatabricksV2)):
    #         # Use the Databricks agent to run it by default.
    #         try:
    #             ctx = FlyteContextManager.current_context()
    #             if not ctx.file_access.is_remote(ctx.file_access.raw_output_prefix):
    #                 raise ValueError(
    #                     "To submit a Databricks job locally,"
    #                     " please set --raw-output-data-prefix to a remote path. e.g. s3://, gcs//, etc."
    #                 )
    #             if ctx.execution_state and ctx.execution_state.is_local_execution():
    #                 return AsyncAgentExecutorMixin.execute(self, **kwargs)
    #         except Exception as e:
    #             click.secho(f"❌ Agent failed to run the task with error: {e}", fg="red")
    #             click.secho("Falling back to local execution", fg="red")
    #     return PythonFunctionTask.execute(self, **kwargs)


TaskPlugins.register_pythontask_plugin(Slurm, SlurmTask)
