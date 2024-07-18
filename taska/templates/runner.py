import json
import logging
import os
import re
import sys
import time
import traceback
from concurrent.futures import Future
from logging.handlers import RotatingFileHandler
from pathlib import Path
from threading import Thread, Timer


class LoggerStream:
    def __init__(self, logger):
        self.logger = logger
        self.linebuf = ""
        self.newline = True

    def write(self, buf):
        if self.newline:
            buf = f'[{time.strftime("%Y-%m-%d %H:%M:%S")}] {buf}'
        if buf.endswith("\n"):
            self.newline = True
        else:
            self.newline = False
        self.logger.info(buf)

    def flush(self):
        pass


class SingletonError(RuntimeError):
    pass


def read_size(text: str):
    # 1g, 1GB, 1g => 1024**3
    m = re.match(r"(\d+)([gGmMkK])?", str(text))
    if not m:
        raise ValueError("Invalid size string: %s" % text)
    a, b = m.groups()
    size = int(a)
    if b:
        size = size * 1024 ** {"g": 3, "m": 2, "k": 1}[b.lower()]
    return size


def is_running_win32(pid: int):
    with os.popen('tasklist /fo csv /fi "pid eq %s"' % int(pid)) as f:
        f.readline()
        text = f.readline()
        return bool(text)


def is_running_linux(pid: int):
    try:
        os.kill(int(pid), 0)
        return True
    except OSError:
        return False
    except SystemError:
        return True


def get_running_pid(pid_file: Path):
    if not pid_file.is_file():
        return
    old_pid = pid_file.read_text()
    if not old_pid:
        return
    if sys.platform == "win32":
        with os.popen('tasklist /fo csv /fi "pid eq %s"' % int(old_pid)) as f:
            f.readline()
            text = f.readline().strip()
            if text:
                return old_pid
    else:
        try:
            os.kill(int(old_pid), 0)
            return old_pid
        except OSError:
            pass
        except SystemError:
            return old_pid


def log_result(result_limit, result_item: dict, start_ts):
    result_item["end_at"] = time.strftime("%Y-%m-%d %H:%M:%S")
    result_item["duration"] = round(time.time() - start_ts, 3)
    result_logger = logging.getLogger("result_logger")
    result_logger.setLevel(logging.DEBUG)
    handler = RotatingFileHandler(
        Path(os.getcwd()).joinpath("result.jsonl").resolve().as_posix(),
        maxBytes=result_limit * 1.1,
        backupCount=1,
        encoding="utf-8",
        errors="replace",
    )
    handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter("%(message)s")
    handler.setFormatter(formatter)
    result_logger.addHandler(handler)
    result_logger.info(json.dumps(result_item, ensure_ascii=False, default=repr))
    handler.flush()
    result_logger.removeHandler(handler)


def setup_stdout_logger(cwd_path, stdout_limit):
    stdout_logger = logging.getLogger("stdout_logger")
    stdout_logger.setLevel(logging.DEBUG)
    handler = RotatingFileHandler(
        cwd_path.joinpath("stdout.log").resolve().as_posix(),
        maxBytes=stdout_limit * 1.1,
        backupCount=1,
        encoding="utf-8",
        errors="replace",
    )
    handler.terminator = ""
    handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter("%(message)s")
    handler.setFormatter(formatter)
    stdout_logger.addHandler(handler)
    stream = LoggerStream(stdout_logger)
    sys.stdout = sys.stderr = stream
    return stdout_logger


def setup_mem_limit(mem_limit: str):
    if sys.platform != "win32" and mem_limit:
        mem_limit = read_size(mem_limit)
        if mem_limit:
            import resource

            resource.setrlimit(resource.RLIMIT_RSS, (mem_limit, mem_limit))


def start_job(entrypoint, params, cwd_path, EXEC_GLOBAL_FUTURE: Future):
    pattern = r"^\w+(\.\w+)?(:\w+)?$"
    if re.match(pattern, entrypoint):
        module, _, function = entrypoint.partition(":")
        if module:
            # main may be: 'module.py:main' or 'module.main' or 'package.module:main'
            # replace module.py to module
            workspace_dir = cwd_path.parent.parent.resolve()
            sys.path.insert(0, workspace_dir.as_posix())
            module_path = workspace_dir / module
            if module_path.is_file():
                module = module_path.stem
            code = f"import {module}"
            if function:
                code += f"; EXEC_GLOBAL_FUTURE.set_result({module}.{function}(**RUNNER_PARAMS))"
            try:
                exec(
                    code,
                    {
                        "EXEC_GLOBAL_FUTURE": EXEC_GLOBAL_FUTURE,
                        "RUNNER_PARAMS": params,
                    },
                )
                if not EXEC_GLOBAL_FUTURE.done():
                    EXEC_GLOBAL_FUTURE.set_exception(
                        RuntimeError("Unknown Error, but no result(not None)")
                    )
            except Exception as e:
                if not EXEC_GLOBAL_FUTURE.done():
                    EXEC_GLOBAL_FUTURE.set_exception(e)

        else:
            EXEC_GLOBAL_FUTURE.set_exception(
                ValueError("Invalid entrypoint: %s" % entrypoint)
            )
    else:
        EXEC_GLOBAL_FUTURE.set_exception(
            ValueError("Invalid entrypoint: %s" % entrypoint)
        )


def main():
    """job_meta:
    {
        "entrypoint": "",
        "params": {},
        "enable": 0,
        "crontab": "",
        "timeout": 0,
        "mem_limit": "",
        "result_limit": "",
        "stdout_limit": ""
    }"""
    cwd_path = Path(os.getcwd()).resolve()
    meta = json.loads(cwd_path.joinpath("meta.json").read_text(encoding="utf-8"))
    default_log_size = 5 * 1024**2
    result_limit = read_size(meta["result_limit"] or default_log_size)
    stdout_limit = read_size(meta["stdout_limit"] or default_log_size)
    start_at = time.strftime("%Y-%m-%d %H:%M:%S")
    start_ts = time.time()
    result_item = {
        "start_at": start_at,
        "result": None,
        "error": None,
    }
    pid_str = str(os.getpid())
    pid_file = cwd_path / "job.pid"
    try:
        thread = None
        running_pid = get_running_pid(pid_file)
        if running_pid:
            raise SingletonError(
                f"Job already running. pid: {pid_str}, running_pid: {running_pid}"
            )
        # start job
        pid_file.write_text(pid_str)
        setup_stdout_logger(cwd_path, stdout_limit)
        print(f"[INFO] Job start. pid: {pid_str}", flush=True)
        setup_mem_limit(meta["mem_limit"])
        EXEC_GLOBAL_FUTURE: Future = Future()
        thread = Thread(
            target=start_job,
            args=(meta["entrypoint"], meta["params"], cwd_path, EXEC_GLOBAL_FUTURE),
            daemon=True,
        )
        thread.start()

        try:
            timeout = meta.get("timeout")
            if not timeout and isinstance(timeout, int):
                timeout = None
            result_item["result"] = EXEC_GLOBAL_FUTURE.result(timeout=timeout)
        except TimeoutError:
            e = TimeoutError(f"timeout={timeout}")
            if not EXEC_GLOBAL_FUTURE.done():
                EXEC_GLOBAL_FUTURE.set_exception(e)
            raise e
    except Exception as e:
        print(
            f"[ERROR] Job fail. pid: {pid_str}, start_at: {start_at}, error: {traceback.format_exc()}",
            flush=True,
        )
        result_item["error"] = repr(e)
    finally:
        log_result(result_limit, result_item, start_ts)
        print(f"[INFO] Job end. pid: {pid_str}, start_at: {start_at}", flush=True)
        if pid_file.is_file() and pid_file.read_text() == pid_str:
            pid_file.write_text("")
        if thread and thread.is_alive():
            timer = Timer(1, lambda: os._exit(1))
            timer.daemon = True
            timer.start()


if __name__ == "__main__":
    main()
