import os
from dataclasses import dataclass
from typing import Optional

import asyncssh

from flytekit.extend.backend.base_agent import AgentRegistry, AsyncAgentBase, Resource, ResourceMeta
from flytekit.extend.backend.utils import convert_to_flyte_phase
from flytekit.models.literals import LiteralMap
from flytekit.models.task import TaskTemplate


# Configure ssh info
class SSHCfg:
    host = os.environ["SSH_HOST"]
    port = int(os.environ["SSH_PORT"])
    username = os.environ["SSH_USERNAME"]
    password = os.environ["SSH_PASSWORD"]


@dataclass
class SlurmJobMetadata(ResourceMeta):
    job_id: str


class SlurmAgent(AsyncAgentBase):
    name = "Slurm Agent"

    def __init__(self) -> None:
        super(SlurmAgent, self).__init__(task_type_name="slurm", metadata_type=SlurmJobMetadata)

    async def create(
        self,
        task_template: TaskTemplate,
        inputs: Optional[LiteralMap] = None,
        **kwargs,
    ) -> SlurmJobMetadata:
        print(task_template)

        print("-" * 50)
        print(task_template.container.args)

        sbatch_conf = task_template.custom["sbatch_conf"]

        # Construct entrypoint for slurm cluster using sbatch_conf?
        cmd = ["sbatch"]
        for opt, val in sbatch_conf.items():
            cmd.extend([f"--{opt}", str(val)])
        cmd = " ".join(cmd)
        cmd += " /tmp/test2.slurm"

        async with asyncssh.connect(
            host=SSHCfg.host, port=SSHCfg.port, username=SSHCfg.username, password=SSHCfg.password
        ) as conn:
            res = await conn.run(cmd, check=True)

        job_id = res.stdout.split()[-1]
        print(job_id)

        return SlurmJobMetadata(job_id=job_id)

    async def get(self, resource_meta: SlurmJobMetadata, **kwargs) -> Resource:
        async with asyncssh.connect(
            host=SSHCfg.host, port=SSHCfg.port, username=SSHCfg.username, password=SSHCfg.password, known_hosts=None
        ) as conn:
            res = await conn.run(f"scontrol show job {resource_meta.job_id}", check=True)

        # Determine the current flyte phase from the Slurm job state
        job_state = "running"
        for o in res.stdout.split(" "):
            if "JobState" in o:
                job_state = o.split("=")[1].strip().lower()
        cur_phase = convert_to_flyte_phase(job_state)
        outputs = {"o0": 2025}  # Redundant, have no idea how to get  now

        # If outputs isn't returned with tt.interface.outputs set, outputs.pb will be loaded
        return Resource(phase=cur_phase, outputs=outputs)

    async def delete(self, resource_meta: SlurmJobMetadata, **kwargs) -> None:
        async with asyncssh.connect(
            host=SSHCfg.host, port=SSHCfg.port, username=SSHCfg.username, password=SSHCfg.password, known_hosts=None
        ) as conn:
            _ = await conn.run(f"scancel {resource_meta.job_id}", check=True)


AgentRegistry.register(SlurmAgent())
