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
        tmp = task_template.container.args
        # tmp[-3] = "home.abaowei.dev.flytekit.build.tiny_slurm"

        srun_conf = task_template.custom["srun_conf"]

        # Construct entrypoint for slurm cluster using srun_conf?
        # cmd = ["srun", "bash", "-c"]
        cmd = ["srun"]
        for opt, val in srun_conf.items():
            cmd.extend([f"--{opt}", str(val)])
        cmd.extend(["bash", "-c"])
        cmd = " ".join(cmd)
        # cmd += " /tmp/test2.slurm"  # Execute a demo script
        # cmd += " 'echo $0'" # Show shell type
        # cmd += " '. ./test.sh'"  # Hard to make 'source activate' work in a script
        entrypoint = " ".join(tmp)
        cmd += f""" 'export PATH=$PATH:/opt/anaconda/anaconda3/bin;
            export FLYTECTL_CONFIG=/home/abaowei/.flyte/config-dev.yaml;
            source activate dev;
            # python3 demo.py;
            {entrypoint};
            echo $SLURM_JOB_ID;'
        """

        async with asyncssh.connect(
            host=SSHCfg.host, port=SSHCfg.port, username=SSHCfg.username, password=SSHCfg.password
        ) as conn:
            res = await conn.run(cmd, check=True)

        # Direct return for sbatch
        # job_id = res.stdout.split()[-1]
        # Use echo trick for srun
        job_id = res.stdout.strip()

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

        # If outputs isn't returned with tt.interface.outputs set, outputs.pb will be loaded
        return Resource(phase=cur_phase)

    async def delete(self, resource_meta: SlurmJobMetadata, **kwargs) -> None:
        async with asyncssh.connect(
            host=SSHCfg.host, port=SSHCfg.port, username=SSHCfg.username, password=SSHCfg.password, known_hosts=None
        ) as conn:
            _ = await conn.run(f"scancel {resource_meta.job_id}", check=True)


AgentRegistry.register(SlurmAgent())
