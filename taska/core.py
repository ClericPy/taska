import json
import logging
import re
import subprocess
import sys
import typing
import venv
from pathlib import Path

from morebuiltins.utils import default_dict

logger = logging.getLogger(__name__)


class PythonDirConf(typing.TypedDict):
    dir_name: str
    executable: str


class VenvDirConf(typing.TypedDict):
    dir_name: str
    requirements: list


class WorkspaceDirConf(typing.TypedDict):
    dir_name: str


class Job(typing.TypedDict):
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
    default_executable = sys.executable
    default_python_dir_name = "default_python"
    # may be pythonw.exe without console
    win32_default_python_name = "python.exe"

    @classmethod
    def prepare_root_dir(cls, target_dir: Path):
        target_dir.mkdir(parents=True, exist_ok=True)
        target_dir.joinpath("runner.py").write_bytes(
            Path(__file__).parent.joinpath("./templates/runner.py").read_bytes()
        )
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
        python_dir.joinpath(cls.executable_file_name).write_text(
            py_path.resolve().as_posix(), encoding="utf-8"
        )
        return python_dir.resolve()

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
        if pip_commands:
            temp_req_file.write_text("\n".join(pip_commands), encoding="utf-8")
            cmd = [
                executable,
                "-m",
                "pip",
                "install",
                "-r",
                temp_req_file.resolve().as_posix(),
            ]
            proc = subprocess.run(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                encoding="utf-8",
                errors="replace",
            )
            if proc.returncode != 0:
                raise RuntimeError(f"pip install failed {proc.stderr}")
            logger.debug(f"[PIP] {cmd}\n{proc.stdout}")
            temp_req_file.rename(req_file)
        else:
            req_file.touch()
        return venv_dir.resolve()

    @classmethod
    def prepare_workspace_dir(
        cls, target_dir: Path, workspace_dir_conf: WorkspaceDirConf
    ):
        workspace_dir = target_dir / "workspaces" / workspace_dir_conf["dir_name"]
        workspace_dir.mkdir(parents=True, exist_ok=True)
        return workspace_dir.resolve()

    @classmethod
    def prepare_job_dir(cls, target_dir: Path, job_dir_conf: JobDirConf):
        job_dir = target_dir / "jobs" / job_dir_conf["dir_name"]
        job_dir.mkdir(parents=True, exist_ok=True)
        job = job_dir_conf["job"]
        job_dir.joinpath("meta.json").write_text(
            json.dumps(job, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
        return job_dir.resolve()

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
        return root_dir, python_dir, venv_dir, workspace_dir, job_dir

    @classmethod
    def prepare_default_env(cls, root_dir: Path, force=False):
        root_dir.mkdir(parents=True, exist_ok=True)
        job: Job = default_dict(Job)
        job["entrypoint"] = "os_system:os_system"
        if sys.platform == "win32":
            job["params"] = {
                "cmd": "wmic os get FreePhysicalMemory,TotalVisibleMemorySize"
            }
        else:
            job["params"] = {"cmd": "df -h"}
        if not force and root_dir.joinpath(cls.default_python_dir_name).is_dir():
            return
        root_dir, python_dir, venv_dir, workspace_dir, job_dir = cls.prepare_all(
            root_dir,
            python_dir_conf={
                "dir_name": cls.default_python_dir_name,
                "executable": cls.default_executable,
            },
            venv_dir_conf={"dir_name": "venv1", "requirements": ["morebuiltins"]},
            workspace_dir_conf={"dir_name": "workspace1"},
            job_dir_conf={"dir_name": "job1", "job": job},
        )

        workspace_dir.joinpath("os_system.py").write_text(r"""
def os_system(cmd):
    import subprocess

    p = subprocess.Popen(
        cmd,
        shell=True,
        stderr=subprocess.STDOUT,
        stdout=subprocess.PIPE,
        text=True,
        errors="replace",
    )
    while True:
        buff = p.stdout.read(1)
        print(buff, end="", flush=True)
        if not buff:
            break
    return p.returncode
""")

    @classmethod
    def launch_job(cls, job_path: typing.Union[Path, str]):
        job_path = Path(job_path).resolve()
        if job_path.is_dir():
            job_dir = job_path
            # job dir -> job meta file
            job_path = job_dir / "meta.json"
        elif job_path.is_file():
            job_dir = job_path.parent
        else:
            raise FileNotFoundError(job_path)
        workspace_dir = job_dir.parent.parent
        venv_dir = workspace_dir.parent.parent
        runner_path = venv_dir.parent.parent / "runner.py"
        assert runner_path.is_file()
        if sys.platform == "win32":
            executable = venv_dir / "Scripts" / cls.win32_default_python_name
        else:
            executable = venv_dir / "bin" / "python"
        cmd = [executable.as_posix(), runner_path.as_posix()]
        if sys.platform == "win32":
            return subprocess.Popen(
                cmd,
                creationflags=subprocess.DETACHED_PROCESS
                | subprocess.CREATE_NEW_PROCESS_GROUP
                | subprocess.CREATE_NO_WINDOW
                | subprocess.CREATE_BREAKAWAY_FROM_JOB,
                cwd=job_dir.as_posix(),
            )
        else:
            return subprocess.Popen(cmd, start_new_session=True, cwd=job_dir.as_posix())


def test():
    root_dir = Path("../demo_path/") / "demo"
    Env.prepare_default_env(root_dir, force=True)


if __name__ == "__main__":
    test()
