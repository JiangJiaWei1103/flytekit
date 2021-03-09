import collections
import datetime
from abc import abstractmethod
from dataclasses import dataclass
from typing import Any, Dict, Generic, Optional, Tuple, Type, TypeVar, Union

from flytekit.common.exceptions import user as _user_exceptions
from flytekit.common.tasks.sdk_runnable import ExecutionParameters
from flytekit.core.context_manager import (
    BranchEvalMode,
    ExecutionState,
    FlyteContext,
    FlyteEntities,
    SerializationSettings,
)
from flytekit.core.interface import Interface, transform_interface_to_typed_interface
from flytekit.core.promise import (
    Promise,
    VoidPromise,
    create_and_link_node,
    create_task_output,
    translate_inputs_to_literals,
)
from flytekit.core.type_engine import TypeEngine
from flytekit.loggers import logger
from flytekit.models import dynamic_job as _dynamic_job
from flytekit.models import interface as _interface_models
from flytekit.models import literals as _literal_models
from flytekit.models import task as _task_model
from flytekit.models.interface import Variable


def kwtypes(**kwargs) -> Dict[str, Type]:
    """
    Converts the keyword arguments to typed dictionary
    """
    d = collections.OrderedDict()
    for k, v in kwargs.items():
        d[k] = v
    return d


@dataclass
class TaskMetadata(object):
    """
    Create Metadata to be associated with this Task

    Args:
      cache: Boolean that indicates if caching should be enabled
      cache_version: Version string to be used for the cached value
      interruptable: Boolean that indicates that this task can be interrupted and/or scheduled on nodes
                     with lower QoS guarantees. This will directly reduce the `$`/`execution cost` associated,
                     at the cost of performance penalties due to potential interruptions
      deprecated: A string that can be used to provide a warning message for deprecated task. Absence / empty str
                  indicates that the task is active and not deprecated
      retries: for retries=n; n > 0, on failures of this task, the task will be retried at-least n number of times.
      timeout: the max amount of time for which one execution of this task should be executed for. If the execution
               will be terminated if the runtime exceeds the given timeout (approximately)
     """

    cache: bool = False
    cache_version: str = ""
    interruptable: bool = False
    deprecated: str = ""
    retries: int = 0
    timeout: Optional[Union[datetime.timedelta, int]] = None

    def __post_init__(self):
        if self.timeout:
            if isinstance(self.timeout, int):
                self.timeout = datetime.timedelta(seconds=self.timeout)
            elif not isinstance(self.timeout, datetime.timedelta):
                raise ValueError("timeout should be duration represented as either a datetime.timedelta or int seconds")
        if self.cache and not self.cache_version:
            raise ValueError("Caching is enabled ``cache=True`` but ``cache_version`` is not set.")

    @property
    def retry_strategy(self) -> _literal_models.RetryStrategy:
        return _literal_models.RetryStrategy(self.retries)

    def to_taskmetadata_model(self) -> _task_model.TaskMetadata:
        """
        Converts to _task_model.TaskMetadata
        """
        return _task_model.TaskMetadata(
            discoverable=self.cache,
            # TODO Fix the version circular dependency before beta
            runtime=_task_model.RuntimeMetadata(_task_model.RuntimeMetadata.RuntimeType.FLYTE_SDK, "0.16.0", "python"),
            timeout=self.timeout,
            retries=self.retry_strategy,
            interruptible=self.interruptable,
            discovery_version=self.cache_version,
            deprecated_error_message=self.deprecated,
        )


class IgnoreOutputs(Exception):
    """
    This exception should be used to indicate that the outputs generated by this can be safely ignored.
    This is useful in case of distributed training or peer-to-peer parallel algorithms.

    For example look at Sagemaker training.
    """

    pass


class Task(object):
    """
    The base of all Tasks in flytekit. This task is closest to the FlyteIDL TaskTemplate and captures information in
    FlyteIDL specification and does not have python native interfaces associated. For any real extension please
    refer to the derived classes.
    """

    def __init__(
        self,
        task_type: str,
        name: str,
        interface: Optional[_interface_models.TypedInterface] = None,
        metadata: Optional[TaskMetadata] = None,
        task_type_version=0,
        **kwargs,
    ):
        self._task_type = task_type
        self._name = name
        self._interface = interface
        self._metadata = metadata if metadata else TaskMetadata()
        self._task_type_version = task_type_version

        FlyteEntities.entities.append(self)

    @property
    def interface(self) -> Optional[_interface_models.TypedInterface]:
        return self._interface

    @property
    def metadata(self) -> TaskMetadata:
        return self._metadata

    @property
    def name(self) -> str:
        return self._name

    @property
    def task_type(self) -> str:
        return self._task_type

    @property
    def python_interface(self) -> Optional[Interface]:
        return None

    @property
    def task_type_version(self) -> int:
        return self._task_type_version

    def get_type_for_input_var(self, k: str, v: Any) -> type:
        """
        Returns the python native type for the given input variable
        # TODO we could use literal type to determine this
        """
        return type(v)

    def get_type_for_output_var(self, k: str, v: Any) -> type:
        """
        Returns the python native type for the given output variable
        # TODO we could use literal type to determine this
        """
        return type(v)

    def get_input_types(self) -> Dict[str, type]:
        """
        Returns python native types for inputs. In case this is not a python native task (base class) and hence
        returns a None. we could deduce the type from literal types, but that is not a required excercise
        # TODO we could use literal type to determine this
        """
        return None

    def _local_execute(self, ctx: FlyteContext, **kwargs) -> Union[Tuple[Promise], Promise, VoidPromise]:
        """
        This code is used only in the case when we want to dispatch_execute with outputs from a previous node
        For regular execution, dispatch_execute is invoked directly.
        """
        # Unwrap the kwargs values. After this, we essentially have a LiteralMap
        # The reason why we need to do this is because the inputs during local execute can be of 2 types
        #  - Promises or native constants
        #  Promises as essentially inputs from previous task executions
        #  native constants are just bound to this specific task (default values for a task input)
        #  Also alongwith promises and constants, there could be dictionary or list of promises or constants
        kwargs = translate_inputs_to_literals(
            ctx,
            incoming_values=kwargs,
            flyte_interface_types=self.interface.inputs,
            native_types=self.get_input_types(),
        )
        input_literal_map = _literal_models.LiteralMap(literals=kwargs)

        outputs_literal_map = self.dispatch_execute(ctx, input_literal_map)
        outputs_literals = outputs_literal_map.literals

        # TODO maybe this is the part that should be done for local execution, we pass the outputs to some special
        #    location, otherwise we dont really need to right? The higher level execute could just handle literalMap
        # After running, we again have to wrap the outputs, if any, back into Promise objects
        output_names = list(self.interface.outputs.keys())
        if len(output_names) != len(outputs_literals):
            # Length check, clean up exception
            raise AssertionError(f"Length difference {len(output_names)} {len(outputs_literals)}")

        # Tasks that don't return anything still return a VoidPromise
        if len(output_names) == 0:
            return VoidPromise(self.name)

        vals = [Promise(var, outputs_literals[var]) for var in output_names]
        return create_task_output(vals, self.python_interface)

    def __call__(self, *args, **kwargs):
        # When a Task is () aka __called__, there are three things we may do:
        #  a. Task Execution Mode - just run the Python function as Python normally would. Flyte steps completely
        #     out of the way.
        #  b. Compilation Mode - this happens when the function is called as part of a workflow (potentially
        #     dynamic task?). Instead of running the user function, produce promise objects and create a node.
        #  c. Workflow Execution Mode - when a workflow is being run locally. Even though workflows are functions
        #     and everything should be able to be passed through naturally, we'll want to wrap output values of the
        #     function into objects, so that potential .with_cpu or other ancillary functions can be attached to do
        #     nothing. Subsequent tasks will have to know how to unwrap these. If by chance a non-Flyte task uses a
        #     task output as an input, things probably will fail pretty obviously.
        if len(args) > 0:
            raise _user_exceptions.FlyteAssertion(
                f"When calling tasks, only keyword args are supported. "
                f"Aborting execution as detected {len(args)} positional args {args}"
            )

        ctx = FlyteContext.current_context()
        if ctx.compilation_state is not None and ctx.compilation_state.mode == 1:
            return self.compile(ctx, *args, **kwargs)
        elif (
            ctx.execution_state is not None and ctx.execution_state.mode == ExecutionState.Mode.LOCAL_WORKFLOW_EXECUTION
        ):
            if ctx.execution_state.branch_eval_mode == BranchEvalMode.BRANCH_SKIPPED:
                if self.python_interface and self.python_interface.output_tuple_name:
                    variables = [k for k in self.python_interface.outputs.keys()]
                    output_tuple = collections.namedtuple(self.python_interface.output_tuple_name, variables)
                    nones = [None for _ in self.python_interface.outputs.keys()]
                    return output_tuple(*nones)
                else:
                    # Should we return multiple None's here?
                    return None
            return self._local_execute(ctx, **kwargs)
        else:
            logger.warning("task run without context - executing raw function")
            new_user_params = self.pre_execute(ctx.user_space_params)
            with ctx.new_execution_context(
                mode=ExecutionState.Mode.LOCAL_TASK_EXECUTION, execution_params=new_user_params
            ):
                return self.execute(**kwargs)

    def compile(self, ctx: FlyteContext, *args, **kwargs):
        raise Exception("not implemented")

    def get_container(self, settings: SerializationSettings) -> _task_model.Container:
        return None

    def get_custom(self, settings: SerializationSettings) -> Dict[str, Any]:
        return None

    @abstractmethod
    def dispatch_execute(
        self, ctx: FlyteContext, input_literal_map: _literal_models.LiteralMap,
    ) -> _literal_models.LiteralMap:
        """
        This method translates Flyte's Type system based input values and invokes the actual call to the executor
        This method is also invoked during runtime.
        """
        pass

    @abstractmethod
    def pre_execute(self, user_params: ExecutionParameters) -> ExecutionParameters:
        """
        This is the method that will be invoked directly before executing the task method and before all the inputs
        are converted. One particular case where this is useful is if the context is to be modified for the user process
        to get some user space parameters. This also ensures that things like SparkSession are already correctly
        setup before the type transformers are called

        This should return either the same context of the mutated context
        """
        pass

    @abstractmethod
    def execute(self, **kwargs) -> Any:
        pass


T = TypeVar("T")


class PythonTask(Task, Generic[T]):
    """
    Base Class for all Tasks with a Python native ``Interface``. This should be directly used for task types, that do
    not have a python function to be executed. Otherwise refer to :py:class:`flytekit.PythonFunctionTask`.
    """

    def __init__(
        self,
        task_type: str,
        name: str,
        task_config: T,
        interface: Optional[Interface] = None,
        environment=None,
        task_type_version=0,
        **kwargs,
    ):
        super().__init__(
            task_type=task_type,
            name=name,
            interface=transform_interface_to_typed_interface(interface),
            task_type_version=task_type_version,
            **kwargs,
        )
        self._python_interface = interface if interface else Interface()
        self._environment = environment if environment else {}
        self._task_config = task_config

    # TODO lets call this interface and the other as flyte_interface?
    @property
    def python_interface(self) -> Interface:
        return self._python_interface

    @property
    def task_config(self) -> T:
        return self._task_config

    def get_type_for_input_var(self, k: str, v: Any) -> Optional[Type[Any]]:
        return self._python_interface.inputs[k]

    def get_type_for_output_var(self, k: str, v: Any) -> Optional[Type[Any]]:
        return self._python_interface.outputs[k]

    def get_input_types(self) -> Optional[Dict[str, type]]:
        return self._python_interface.inputs

    def compile(self, ctx: FlyteContext, *args, **kwargs):
        return create_and_link_node(
            ctx,
            entity=self,
            interface=self.python_interface,
            timeout=self.metadata.timeout,
            retry_strategy=self.metadata.retry_strategy,
            **kwargs,
        )

    @property
    def _outputs_interface(self) -> Dict[Any, Variable]:
        return self.interface.outputs

    def dispatch_execute(
        self, ctx: FlyteContext, input_literal_map: _literal_models.LiteralMap
    ) -> Union[_literal_models.LiteralMap, _dynamic_job.DynamicJobSpec]:
        """
        This method translates Flyte's Type system based input values and invokes the actual call to the executor
        This method is also invoked during runtime.

        * ``VoidPromise`` is returned in the case when the task itself declares no outputs.
        * ``Literal Map`` is returned when the task returns either one more outputs in the declaration. Individual outputs
          may be none
        * ``DynamicJobSpec`` is returned when a dynamic workflow is executed
        """

        # Invoked before the task is executed
        new_user_params = self.pre_execute(ctx.user_space_params)

        # Create another execution context with the new user params, but let's keep the same working dir
        with ctx.new_execution_context(
            mode=ctx.execution_state.mode,
            execution_params=new_user_params,
            working_dir=ctx.execution_state.working_dir,
        ) as exec_ctx:
            # TODO We could support default values here too - but not part of the plan right now
            # Translate the input literals to Python native
            native_inputs = TypeEngine.literal_map_to_kwargs(exec_ctx, input_literal_map, self.python_interface.inputs)

            # TODO: Logger should auto inject the current context information to indicate if the task is running within
            #   a workflow or a subworkflow etc
            logger.info(f"Invoking {self.name} with inputs: {native_inputs}")
            native_outputs = None
            try:
                native_outputs = self.execute(**native_inputs)
            except Exception as e:
                logger.exception(f"Exception when executing {e}")
                raise e

            logger.info(f"Task executed successfully in user level, outputs: {native_outputs}")
            # Lets run the post_execute method. This may result in a IgnoreOutputs Exception, which is
            # bubbled up to be handled at the callee layer.
            native_outputs = self.post_execute(new_user_params, native_outputs)

            # Short circuit the translation to literal map because what's returned may be a dj spec (or an
            # already-constructed LiteralMap if the dynamic task was a no-op), not python native values
            if isinstance(native_outputs, _literal_models.LiteralMap) or isinstance(
                native_outputs, _dynamic_job.DynamicJobSpec
            ):
                return native_outputs

            expected_output_names = list(self._outputs_interface.keys())
            if len(expected_output_names) == 1:
                # Here we have to handle the fact that the task could've been declared with a typing.NamedTuple of
                # length one. That convention is used for naming outputs - and single-length-NamedTuples are
                # particularly troublesome but elegant handling of them is not a high priority
                # Again, we're using the output_tuple_name as a proxy.
                if self.python_interface.output_tuple_name and isinstance(native_outputs, tuple):
                    native_outputs_as_map = {expected_output_names[0]: native_outputs[0]}
                else:
                    native_outputs_as_map = {expected_output_names[0]: native_outputs}
            elif len(expected_output_names) == 0:
                native_outputs_as_map = {}
            else:
                native_outputs_as_map = {
                    expected_output_names[i]: native_outputs[i] for i, _ in enumerate(native_outputs)
                }

            # We manually construct a LiteralMap here because task inputs and outputs actually violate the assumption
            # built into the IDL that all the values of a literal map are of the same type.
            literals = {}
            for k, v in native_outputs_as_map.items():
                literal_type = self._outputs_interface[k].type
                py_type = self.get_type_for_output_var(k, v)

                if isinstance(v, tuple):
                    raise AssertionError(f"Output({k}) in task{self.name} received a tuple {v}, instead of {py_type}")
                try:
                    literals[k] = TypeEngine.to_literal(exec_ctx, v, py_type, literal_type)
                except Exception as e:
                    raise AssertionError(f"failed to convert return value for var {k}") from e

            outputs_literal_map = _literal_models.LiteralMap(literals=literals)
            # After the execute has been successfully completed
            return outputs_literal_map

    def pre_execute(self, user_params: ExecutionParameters) -> ExecutionParameters:
        """
        This is the method that will be invoked directly before executing the task method and before all the inputs
        are converted. One particular case where this is useful is if the context is to be modified for the user process
        to get some user space parameters. This also ensures that things like SparkSession are already correctly
        setup before the type transformers are called

        This should return either the same context of the mutated context
        """
        return user_params

    @abstractmethod
    def execute(self, **kwargs) -> Any:
        pass

    def post_execute(self, user_params: ExecutionParameters, rval: Any) -> Any:
        """
        Post execute is called after the execution has completed, with the user_params and can be used to clean-up,
        or alter the outputs to match the intended tasks outputs. If not overriden, then this function is a No-op

        Args:
            rval is returned value from call to execute
            user_params: are the modified user params as created during the pre_execute step
        """
        return rval

    @property
    def environment(self) -> Dict[str, str]:
        return self._environment