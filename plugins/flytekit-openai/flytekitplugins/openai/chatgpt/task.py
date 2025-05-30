from typing import Any, Dict, Optional

from flytekit.configuration import SerializationSettings
from flytekit.core.base_task import PythonTask
from flytekit.core.interface import Interface
from flytekit.extend.backend.base_connector import SyncConnectorExecutorMixin


class ChatGPTTask(SyncConnectorExecutorMixin, PythonTask):
    """
    This is the simplest form of a ChatGPT Task, you can define the model and the input you want.
    """

    _TASK_TYPE = "chatgpt"

    def __init__(self, name: str, chatgpt_config: Dict[str, Any], openai_organization: Optional[str] = None, **kwargs):
        """
        Args:
            name: Name of this task, should be unique in the project
            openai_organization: OpenAI Organization. String can be found here. https://platform.openai.com/docs/api-reference/organization-optional
            chatgpt_config: ChatGPT job configuration. Config structure can be found here. https://platform.openai.com/docs/api-reference/completions/create
        """

        if "model" not in chatgpt_config:
            raise ValueError("The 'model' configuration variable is required in chatgpt_config")

        task_config = {"openai_organization": openai_organization, "chatgpt_config": chatgpt_config}

        inputs = {"message": str}
        outputs = {"o0": str}

        super().__init__(
            task_type=self._TASK_TYPE,
            name=name,
            task_config=task_config,
            interface=Interface(inputs=inputs, outputs=outputs),
            **kwargs,
        )

    def get_custom(self, settings: SerializationSettings) -> Dict[str, Any]:
        return {
            "openai_organization": self.task_config["openai_organization"],
            "chatgpt_config": self.task_config["chatgpt_config"],
        }
