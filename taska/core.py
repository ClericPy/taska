import abc
import json
import logging
import re
import shutil
import signal
import subprocess
import sys
import typing
import venv
from datetime import datetime
from pathlib import Path

from morebuiltins.date import Crontab
from morebuiltins.utils import default_dict, is_running

logger = logging.getLogger(__name__)


class Job(typing.TypedDict):
    name: str
    description: str
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


class DirBase(abc.ABC):
    @classmethod
    @abc.abstractmethod
    def prepare_dir(
        cls, target_dir: Path, name: str = "", force=False, **kwargs
    ) -> Path:
        raise NotImplementedError()

    @classmethod
    def is_valid(cls, path: Path):
        raise NotImplementedError()


class RootDir(DirBase):
    @classmethod
    def prepare_dir(cls, target_dir: Path, name: str = "root", force=False, **kwargs):
        root_dir = target_dir / name
        if not force and cls.is_valid(root_dir):
            return root_dir.resolve()
        root_dir.mkdir(parents=True, exist_ok=True)
        root_dir.joinpath("pids").mkdir(parents=True, exist_ok=True)
        root_dir.joinpath("runner.py").write_bytes(
            Path(__file__).parent.joinpath("./templates/runner.py").read_bytes()
        )
        assert cls.is_valid(root_dir)
        return root_dir.resolve()

    @classmethod
    def is_valid(cls, path: Path):
        return path.joinpath("runner.py").is_file()


class PythonDir(DirBase):
    @classmethod
    def prepare_dir(cls, target_dir: Path, name: str = "", force=False, **kwargs):
        python_dir = target_dir / name
        if not force and cls.is_valid(python_dir):
            return python_dir.resolve()
        python: str = kwargs.get("python", sys.executable)
        python_path = Path(python)
        if not python_path.exists():
            raise FileNotFoundError(str(python))
        if not name:
            name = re.sub(
                r'[<>:"/\\|?*]', "_", python_path.with_suffix("").resolve().as_posix()
            )
        python_dir.mkdir(parents=True, exist_ok=True)
        python_dir.joinpath("python_path").write_text(
            python_path.resolve().as_posix(), encoding="utf-8"
        )
        assert cls.is_valid(python_dir)
        return python_dir.resolve()

    @classmethod
    def is_valid(cls, path: Path):
        return path.joinpath("python_path").is_file()


class VenvDir(DirBase):
    @classmethod
    def prepare_dir(cls, target_dir: Path, name="", force=False, **kwargs):
        venv_dir = target_dir / name
        if not force and cls.is_valid(venv_dir):
            return venv_dir.resolve()
        pip_commands: typing.List[str] = kwargs.get("requirements", [])
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
        assert cls.is_valid(venv_dir)
        return venv_dir.resolve()

    @classmethod
    def is_valid(cls, path: Path):
        return path.joinpath("requirements.txt").is_file()


class WorkspaceDir(DirBase):
    @classmethod
    def prepare_dir(cls, target_dir: Path, name: str = "", force=False, **kwargs):
        workspace_dir = target_dir / "workspaces" / name
        if not force and cls.is_valid(workspace_dir):
            return workspace_dir.resolve()
        workspace_dir.mkdir(parents=True, exist_ok=True)
        workspace_dir.joinpath("jobs").mkdir(parents=True, exist_ok=True)
        assert cls.is_valid(workspace_dir)
        return workspace_dir.resolve()

    @classmethod
    def is_valid(cls, path: Path):
        return path.joinpath("jobs").is_dir()


class JobDir(DirBase):
    @classmethod
    def prepare_dir(cls, target_dir: Path, name: str = "", force=False, **kwargs):
        job_dir = target_dir / "jobs" / name
        if not force and cls.is_valid(job_dir):
            return job_dir.resolve()
        job: Job = kwargs["job"]
        job_dir.mkdir(parents=True, exist_ok=True)
        job_dir.joinpath("meta.json").write_text(
            json.dumps(job, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
        assert cls.is_valid(job_dir)
        return job_dir.resolve()

    @classmethod
    def is_valid(cls, path: Path):
        return path.joinpath("meta.json").is_file()


class Taska:
    SHUTDOWN = False
    TREE_LEVELS = [RootDir, PythonDir, VenvDir, WorkspaceDir, JobDir]

    def __init__(self, root_dir: typing.Union[Path, str]):
        self.root_dir = Path(root_dir).resolve()
        self.tree = self.init_dir_tree()
        signal.signal(signalnum=signal.SIGINT, handler=self.handle_shutdown)

    def handle_shutdown(self, *args):
        self.__class__.SHUTDOWN = True

    def need_run(self, now, cron):
        for _ in Crontab.iter_datetimes(cron, start_date=now, max_tries=1):
            return True
        return False

    def get_todos(self) -> typing.List[Path]:
        result = []
        now = datetime.now()
        for path in self.root_dir.rglob("meta.json"):
            job = json.loads(path.read_text(encoding="utf-8"))
            if job["enable"] and job["crontab"] and self.need_run(now, job["crontab"]):
                result.append(path)
        return result

    def init_dir_tree(self):
        result = {}
        for d in self.root_dir.iterdir():
            if PythonDir.is_valid(d):
                py_result = result.setdefault(d.name, {})
                for v in d.iterdir():
                    if VenvDir.is_valid(v):
                        venv_result = py_result.setdefault(v.name, {})
                        for w in v.joinpath("workspaces").iterdir():
                            if WorkspaceDir.is_valid(w):
                                w_result = venv_result.setdefault(w.name, {})
                                for j in w.joinpath("jobs").iterdir():
                                    if JobDir.is_valid(j):
                                        w_result[j.name] = None
        return result

    @classmethod
    def prepare_default_env(cls, root_dir: Path, force=False):
        if not force and root_dir.joinpath("default").is_dir():
            return
        # 1. prepare root dir
        root_dir = RootDir.prepare_dir(root_dir.parent, name=root_dir.name, force=force)
        # 2. prepare python dir
        python_dir = PythonDir.prepare_dir(
            root_dir, "default", force=force, python=sys.executable
        )
        # 3. prepare venv dir
        venv_dir = VenvDir.prepare_dir(
            python_dir, "venv1", force=force, requirements=["morebuiltins"]
        )
        # 4. prepare workspace dir
        workspace_dir = WorkspaceDir.prepare_dir(
            venv_dir, name="workspace1", force=force
        )
        # 4.1 add code
        workspace_dir.joinpath("mycode.py").write_text(
            """import time\n\ndef main(arg): return print(time.strftime('%Y-%m-%d %H:%M:%S'), arg) or 'result'""",
            encoding="utf-8",
        )
        # 5. prepare job dir
        job: Job = default_dict(Job)
        job["name"] = "test"
        job["entrypoint"] = "mycode:main"
        job["params"] = {"arg": "hello world"}
        job_dir = JobDir.prepare_dir(workspace_dir, job["name"], force=force, job=job)
        return root_dir, python_dir, venv_dir, workspace_dir, job_dir

    @classmethod
    def launch_job(cls, job_path_or_dir: typing.Union[Path, str]):
        job_path = Path(job_path_or_dir).resolve()
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
            executable = venv_dir / "Scripts" / "python.exe"
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

    @classmethod
    def safe_rm_dir(cls, path: typing.Union[Path, str]):
        path = Path(path)
        if not path.is_dir():
            return True
        for pid_path in path.rglob("*.pid"):
            try:
                pid = pid_path.read_bytes()
                if is_running(pid):
                    return False
            except FileNotFoundError:
                continue
        shutil.rmtree(path.resolve().as_posix(), ignore_errors=True)
        return not path.is_dir()


def test():
    root_dir = Path("../demo_path/")
    Taska.prepare_default_env(root_dir, force=False)
    ta = Taska(root_dir)
    print(ta.tree)
    print(ta.get_todos())
    print(datetime.now())
    for path in ta.get_todos():
        print(ta.launch_job(path))


if __name__ == "__main__":
    test()
