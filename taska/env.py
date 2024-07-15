import re
import subprocess
import sys
import typing
import json
import venv
from pathlib import Path

from morebuiltins.utils import default_dict


class PythonDirConf(typing.TypedDict):
    dir_name: str
    executable: str


class VenvDirConf(typing.TypedDict):
    dir_name: str
    requirements: list


class WorkspaceDirConf(typing.TypedDict):
    dir_name: str


class Job(typing.TypedDict):
    # const: '/path/to/workspace/'
    cwd: str
    # entrypoint = 'package.module:function'
    entrypoint: str
    # params = { 'key': 'value' }
    params: dict
    # enable = 1, 0; 1 = enable, 0 = disable
    enable: int
    # crontab default = 0, format = '* * * * *'
    crontab: str
    # default = 60s
    timeout: int
    # 1g/1gb/1GB == 1024**3
    mem_limit: str
    result_limit: str
    stdout_limit: str


class JobDirConf(typing.TypedDict):
    dir_name: str
    job: Job


class Env:
    executable_file_name = "python_path"

    @classmethod
    def prepare_root_dir(cls, target_dir: Path):
        target_dir.mkdir(parents=True, exist_ok=True)
        return target_dir.resolve()

    @classmethod
    def prepare_python_dir(cls, target_dir: Path, python_dir_conf: PythonDirConf):
        dir_name, executable = (
            python_dir_conf["dir_name"],
            python_dir_conf["executable"],
        )
        py_path = Path(executable)
        if not py_path.exists():
            raise FileNotFoundError(str(executable))
        if not dir_name:
            dir_name = re.sub(
                r'[<>:"/\\|?*]', "_", py_path.with_suffix("").resolve().as_posix()
            )
        python_dir = target_dir / dir_name
        python_dir.mkdir(parents=True, exist_ok=True)
        (python_dir / cls.executable_file_name).write_text(
            py_path.resolve().as_posix(), encoding="utf-8"
        )
        return python_dir

    @classmethod
    def prepare_venv_dir(cls, target_dir: Path, venv_dir_conf: VenvDirConf):
        pip_commands = venv_dir_conf["requirements"]
        venv_dir = target_dir / venv_dir_conf["dir_name"]
        req_file = venv_dir / "requirements.txt"
        temp_req_file = req_file.with_suffix(".temp")
        builder = venv.EnvBuilder(
            system_site_packages=False,
            clear=True,
            symlinks=False,
            upgrade=False,
            with_pip=True,
            prompt=None,
            upgrade_deps=False,
        )
        builder.create(venv_dir.resolve().as_posix())
        if sys.platform == "win32":
            executable = (venv_dir / "Scripts" / "python.exe").resolve().as_posix()
        else:
            executable = (venv_dir / "bin" / "python").resolve().as_posix()
        (venv_dir / "python_path").write_text(executable, encoding="utf-8")
        if pip_commands:
            temp_req_file.write_text("\n".join(pip_commands), encoding="utf-8")
            proc = subprocess.run(
                [
                    executable,
                    "-m",
                    "pip",
                    "install",
                    "-r",
                    temp_req_file.resolve().as_posix(),
                ],
                capture_output=True,
            )
            if proc.returncode != 0:
                raise RuntimeError(
                    f"pip install failed {proc.stderr.decode('utf-8', 'replace')}"
                )
            # TODO logging
            print(proc.stdout.decode("utf-8", "replace"), flush=True)
            temp_req_file.rename(req_file)
        else:
            req_file.touch()
        return venv_dir

    @classmethod
    def prepare_workspace_dir(
        cls, target_dir: Path, workspace_dir_conf: WorkspaceDirConf
    ):
        workspace_dir = target_dir / "workspaces" / workspace_dir_conf["dir_name"]
        workspace_dir.mkdir(parents=True, exist_ok=True)
        return workspace_dir

    @classmethod
    def prepare_job_dir(cls, target_dir: Path, job_dir_conf: JobDirConf):
        job_dir = target_dir / "jobs" / job_dir_conf["dir_name"]
        job_dir.mkdir(parents=True, exist_ok=True)
        job = job_dir_conf["job"]
        job["cwd"] = job_dir.parent.parent.resolve().as_posix()
        (job_dir / "meta.json").write_text(
            json.dumps(job, indent=2, ensure_ascii=False)
        )
        return job_dir

    @classmethod
    def prepare_all(
        cls,
        root_dir: Path,
        python_dir_conf: PythonDirConf,
        venv_dir_conf: VenvDirConf,
        workspace_dir_conf: WorkspaceDirConf,
        job_dir_conf: JobDirConf,
    ):
        # 1. prepare root dir
        root_dir = cls.prepare_root_dir(root_dir)
        # 2. prepare python dir
        python_dir = cls.prepare_python_dir(root_dir, python_dir_conf)
        # 3. prepare venv dir
        venv_dir = cls.prepare_venv_dir(python_dir, venv_dir_conf)
        # 4. prepare workspace dir
        workspace_dir = cls.prepare_workspace_dir(venv_dir, workspace_dir_conf)
        # 5. prepare job dir
        job_dir = cls.prepare_job_dir(workspace_dir, job_dir_conf)


def test():
    root_dir = Path("../demo_path/") / "demo"
    job = default_dict(Job)
    # {'cwd': '', 'entrypoint': '', 'params': {}, 'enable': 0, 'crontab': '', 'timeout': 0, 'mem_limit': '', 'result_limit': '', 'stdout_limit': ''}
    Env.prepare_all(
        root_dir,
        python_dir_conf={"dir_name": "default_python", "executable": sys.executable},
        venv_dir_conf={"dir_name": "venv1", "requirements": ["morebuiltins"]},
        workspace_dir_conf={"dir_name": "workspace1"},
        job_dir_conf={"dir_name": "job1", "job": job},
    )


if __name__ == "__main__":
    test()
