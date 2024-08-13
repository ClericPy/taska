import abc
import json
import logging
import os
import re
import shutil
import signal
import subprocess
import sys
import time
import typing
import venv
from datetime import datetime, timedelta
from hashlib import md5
from pathlib import Path

from morebuiltins.date import Crontab
from morebuiltins.utils import is_running, ptime, read_size, read_time, ttime
from psutil import NoSuchProcess, Process

logger = logging.getLogger("taska")


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

    @classmethod
    def get_dir_type(cls, path: Path, default="Dir"):
        for d in [RootDir, PythonDir, VenvDir, WorkspaceDir, JobDir]:
            if d.is_valid(path):
                return d.__name__.replace("Dir", "")
        return default


class RootDir(DirBase):
    @classmethod
    def prepare_dir(cls, target_dir: Path, name: str = "root", force=False, **kwargs):
        root_dir = target_dir / name
        if not force and cls.is_valid(root_dir):
            return root_dir.resolve()
        logger.info(f"[Init] Creating root_dir: {root_dir.resolve().as_posix()}")
        root_dir.mkdir(parents=True, exist_ok=True)
        root_dir.joinpath("pids").mkdir(parents=True, exist_ok=True)
        runner_code = Path(__file__).parent.joinpath("templates/runner.py").read_bytes()
        root_dir.joinpath("runner.py").write_bytes(runner_code)
        root_dir.joinpath("max_workers").write_text(
            str(os.cpu_count()), encoding="utf-8"
        )
        assert cls.is_valid(root_dir)
        return root_dir.resolve()

    @classmethod
    def is_valid(cls, path: Path):
        for name in ("runner.py", "pids", "max_workers"):
            if not path.joinpath(name).exists():
                return False
        return True


class PythonDir(DirBase):
    @classmethod
    def prepare_dir(cls, target_dir: Path, name: str = "", force=False, **kwargs):
        python_dir = target_dir / name
        if not force and cls.is_valid(python_dir):
            return python_dir.resolve()
        logger.info(f"[Init] Creating python_dir: {python_dir.resolve().as_posix()}")
        python: str = kwargs.get("python") or sys.executable
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
            cls.ensure_pip_install(venv_dir)
            return venv_dir.resolve()
        logger.info(f"[Init] Creating venv_dir: {venv_dir.resolve().as_posix()}")

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
        pips = kwargs.get("pips") or []
        venv_dir.joinpath("requirements.txt").write_text("\n".join(pips))
        # PIP_CONFIG_FILE
        venv_dir.joinpath("pip.conf").write_text(
            "[global]\ntimeout = 60", encoding="utf-8"
        )
        venv_dir.joinpath("workspaces").mkdir()
        cls.ensure_pip_install(venv_dir)
        assert cls.is_valid(venv_dir)
        return venv_dir.resolve()

    @classmethod
    def pip_install(cls, venv_dir: Path):
        req_file = venv_dir / "requirements.txt"
        if sys.platform == "win32":
            executable = (venv_dir / "Scripts" / "python.exe").resolve().as_posix()
        else:
            executable = (venv_dir / "bin" / "python").resolve().as_posix()
        cmd = [
            executable,
            "-m",
            "pip",
            "install",
            "-r",
            req_file.resolve().as_posix(),
        ]
        env = os.environ.copy()
        env["PIP_CONFIG_FILE"] = venv_dir.joinpath("pip.conf").resolve().as_posix()
        proc = subprocess.run(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            encoding="utf-8",
            errors="replace",
            env=env,
        )
        if proc.returncode != 0:
            raise RuntimeError(f"pip install failed {proc.stderr}")
        logger.debug(f"[PIP] {cmd}\n{proc.stdout}")
        return True

    @classmethod
    def ensure_pip_install(cls, path: Path):
        r = path / "requirements.txt"
        m = path / "requirements.md5"
        old_md5 = m.read_text(encoding="utf-8") if m.is_file() else ""
        current_md5 = md5(r.read_bytes()).hexdigest()
        ok = False
        if old_md5 != current_md5:
            m.unlink(missing_ok=True)
            if r.is_file():
                requirements = r.read_text(encoding="utf-8")
            else:
                requirements = ""
            if requirements:
                req_str = requirements.replace("\n", ", ")
                logger.info(f"[Pip] Pip install: {r.resolve().as_posix()} {req_str}")
                ok = cls.pip_install(path)
            else:
                ok = True
            if ok:
                m.write_text(current_md5, encoding="utf-8")
        return str(ok)

    @classmethod
    def is_valid(cls, path: Path):
        return (
            path.joinpath("requirements.txt").is_file()
            and path.joinpath("pip.conf").is_file()
        )


class WorkspaceDir(DirBase):
    @classmethod
    def prepare_dir(cls, target_dir: Path, name: str = "", force=False, **kwargs):
        workspace_dir = target_dir / name
        if not force and cls.is_valid(workspace_dir):
            return workspace_dir.resolve()
        logger.info(
            f"[Init] Creating workspace_dir: {workspace_dir.resolve().as_posix()}"
        )
        workspace_dir.mkdir(parents=True, exist_ok=True)
        workspace_dir.joinpath("jobs").mkdir(parents=True, exist_ok=True)
        assert cls.is_valid(workspace_dir)
        return workspace_dir.resolve()

    @classmethod
    def is_valid(cls, path: Path):
        return path.joinpath("jobs").is_dir()


class JobDir(DirBase):
    default_meta = {
        "name": "job_name",
        "description": "",
        "entrypoint": "",
        "params": {},
        "enable": 0,
        "crontab": "* * * * *",
        "timeout": 0,
        "mem_limit": "",
        "result_limit": "",
        "stdout_limit": "",
    }

    @classmethod
    def prepare_dir(cls, target_dir: Path, name: str = "", force=False, **kwargs):
        job_dir = target_dir / name
        if not force and cls.is_valid(job_dir):
            return job_dir.resolve()
        logger.info(f"[Init] Creating job_dir: {job_dir.resolve().as_posix()}")
        job_dir.mkdir(parents=True, exist_ok=True)
        meta = dict(cls.default_meta)
        meta["name"] = name
        job_dir.joinpath("meta.json").write_text(
            json.dumps(meta, indent=2, ensure_ascii=False), encoding="utf-8"
        )
        assert cls.is_valid(job_dir)
        return job_dir.resolve()

    @classmethod
    def is_valid(cls, path: Path):
        return path.joinpath("meta.json").is_file()


class Taska:
    SHUTDOWN = False
    ROOT_PATH: typing.Optional[Path] = None
    TREE_LEVELS = [RootDir, PythonDir, VenvDir, WorkspaceDir, JobDir]
    LATEST_PROC_CACHE: dict = {}
    CACHE_LENGTH = 50

    def __init__(self):
        if self.ROOT_PATH is None:
            raise ValueError("Taska.ROOT_PATH is not set")
        self.root_dir = self.ROOT_PATH
        self.tree = self.init_dir_tree()
        signal.signal(signalnum=signal.SIGINT, handler=self.handle_shutdown)
        signal.signal(signalnum=signal.SIGTERM, handler=self.handle_shutdown)

    def run_forever(self):
        current_min = time.strftime("%M")
        next_min = (datetime.now() + timedelta(minutes=1)).replace(
            second=0, microsecond=0
        )
        logger.warning(
            f"[Start] Program start, pid={os.getpid()}, root_dir={self.root_dir.resolve().as_posix()}"
        )
        while not self.SHUTDOWN:
            _min = time.strftime("%M")
            if _min != current_min:
                current_min = _min
                self.run_once()
                next_min = (datetime.now() + timedelta(minutes=1)).replace(
                    second=0, microsecond=0
                )
            timeleft = next_min.timestamp() - time.time()
            interval = min((1, timeleft))
            if interval > 0:
                time.sleep(interval)
        logger.warning("[End] Program shutdown")

    def run_once(self):
        for job, path in self.get_todos():
            logger.info(
                f"[Launch] Launch job `{job['name']}`: {path.resolve().as_posix()}"
            )
            self.launch_job(path)

    def handle_shutdown(self, *args):
        self.__class__.SHUTDOWN = True
        logger.warning(f"[Shutdown] received shutdown signal: {args[0]}")

    def need_run(self, now, cron):
        for _ in Crontab.iter_datetimes(cron, start_date=now, max_tries=1):
            return True
        return False

    def get_todos(
        self, now: typing.Optional[datetime] = None
    ) -> typing.Iterator[typing.Tuple[Job, Path]]:
        now = now or datetime.now()
        for path in self.root_dir.rglob("meta.json"):
            job = json.loads(path.read_text(encoding="utf-8"))
            if job["enable"] and job["crontab"] and self.need_run(now, job["crontab"]):
                yield job, path

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
        # 1. prepare root dir
        root_dir = RootDir.prepare_dir(root_dir.parent, name=root_dir.name, force=force)
        # 2. prepare python dir
        python_dir = PythonDir.prepare_dir(
            root_dir, "default", force=force, python=sys.executable
        )
        # 3. prepare venv dir
        venv_dir = VenvDir.prepare_dir(
            python_dir, "venv1", force=force, pips=["morebuiltins"]
        )
        VenvDir.ensure_pip_install(venv_dir)
        # 4. prepare workspace dir
        workspace_dir = WorkspaceDir.prepare_dir(
            venv_dir.joinpath("workspaces"), name="workspace1", force=force
        )
        # 4.1 add code
        code_path = workspace_dir.joinpath("mycode.py")
        if force or not code_path.is_file():
            code_path.parent.mkdir(parents=True, exist_ok=True)
            code_path.write_text(
                """import time\n\ndef main(arg): return print(time.strftime('%Y-%m-%d %H:%M:%S'), arg) or 'result'""",
                encoding="utf-8",
            )
        # 5. prepare job dir
        job_dir = JobDir.prepare_dir(workspace_dir / "jobs", "job1", force=force)
        return root_dir, python_dir, venv_dir, workspace_dir, job_dir

    @classmethod
    def launch_job(cls, job_path_or_dir: typing.Union[Path, str], timeout=0) -> Path:
        job_path = Path(job_path_or_dir).resolve()
        if job_path.is_dir() and job_path.joinpath("meta.json").is_file():
            job_dir = job_path
        elif job_path.is_file() and job_path.parent.joinpath("meta.json").is_file():
            job_dir = job_path.parent
        else:
            raise FileNotFoundError(job_path)
        if not JobDir.is_valid(job_dir):
            raise FileNotFoundError
        # job dir -> job meta file
        job_path = job_dir / "meta.json"
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
            proc = subprocess.Popen(
                cmd,
                creationflags=subprocess.DETACHED_PROCESS
                | subprocess.CREATE_NEW_PROCESS_GROUP
                | subprocess.CREATE_NO_WINDOW
                | subprocess.CREATE_BREAKAWAY_FROM_JOB,
                cwd=job_dir.as_posix(),
            )
        else:
            proc = subprocess.Popen(cmd, start_new_session=True, cwd=job_dir.as_posix())
        setattr(proc, "_child_created", False)
        pid_path = job_dir / "pid.txt"
        pid = None
        for _ in range((timeout or 1) * 10):
            try:
                try:
                    if not pid:
                        pid = int(pid_path.read_bytes() or 0)
                        items: dict = cls.get_pids_info([pid])
                        cls.LATEST_PROC_CACHE.update(items)
                        jobs = sorted(
                            cls.LATEST_PROC_CACHE.items(),
                            key=lambda x: x[1]["start_at"],
                        )
                        for k, v in jobs:
                            if v["status"] == "running":
                                try:
                                    v["status"] = Process(k).status()
                                except NoSuchProcess:
                                    v["status"] = "dead"
                            elif len(cls.LATEST_PROC_CACHE) > cls.CACHE_LENGTH:
                                cls.LATEST_PROC_CACHE.pop(k, None)
                except FileNotFoundError:
                    pass
                if proc.wait(0.1):
                    if pid:
                        item = Taska.LATEST_PROC_CACHE[pid]
                        item["status"] = "dead"
                        item["start_at"], item["end_at"], item["elapsed"] = (
                            Taska.get_end_at(item)
                        )
                    break
            except subprocess.TimeoutExpired:
                pass

        del proc
        return job_dir

    @classmethod
    def safe_rm_dir(cls, path: typing.Union[Path, str]):
        path = Path(path)
        if not path.is_dir():
            return True
        for pid_path in path.rglob("pid.txt"):
            try:
                pid = pid_path.read_bytes()
                if is_running(pid):
                    return False
            except FileNotFoundError:
                continue
        shutil.rmtree(path.resolve().as_posix(), ignore_errors=True)
        return not path.is_dir()

    @classmethod
    def get_pids_info(cls, pids: typing.List[int]):
        items: dict = {}
        root_path = cls.ROOT_PATH
        if root_path is None:
            raise RuntimeError("Taska.ROOT_PATH is not set")
        now = time.time()
        for pid in pids:
            try:
                proc = Process(pid)
                status = proc.status()
                job_dir = Path(proc.cwd()).relative_to(root_path).as_posix()
                start_at = ttime(proc.create_time())
                elapsed = read_time(now - proc.create_time(), shorten=True)
                memory = read_size(proc.memory_info().rss, shorten=True)
            except NoSuchProcess:
                proc = None
                job_dir = status = start_at = elapsed = memory = "-"
            item = {
                "pid": pid,
                "status": status,
                "job_dir": job_dir,
                "start_at": start_at,
                "elapsed": elapsed,
                "memory": memory,
            }
            items[pid] = item
        return items

    @classmethod
    def get_end_at(cls, item):
        if not cls.ROOT_PATH:
            raise ValueError("Taska.ROOT_PATH is not set")
        result_path = cls.ROOT_PATH.joinpath(item["job_dir"]).joinpath("result.jsonl")
        result = (item["start_at"], "-", "-")
        if result_path.is_file():
            with open(result_path, "r", encoding="utf-8") as f:
                pid = item["pid"]
                regex = re.compile(f'"pid": ?{pid},')
                for line in f:
                    if regex.search(line):
                        data = json.loads(line)
                        if data.get("pid") == pid:
                            result = (
                                data["start_at"],
                                data["end_at"],
                                read_time(
                                    ptime(data["end_at"]) - ptime(data["start_at"]),
                                    shorten=True,
                                ),
                            )
        return result


def test():
    Taska.ROOT_PATH = Path("../demo_path/").resolve()
    Taska.prepare_default_env(Taska.ROOT_PATH, force=False)
    ta = Taska()
    print(ta.tree)
    print(list(ta.get_todos()))
    print(datetime.now())
    for job, path in ta.get_todos():
        print(job, "launch")
        ta.launch_job(path)


if __name__ == "__main__":
    test()
