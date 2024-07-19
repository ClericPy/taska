from argparse import ArgumentParser
from pathlib import Path

from taska.config import Config
from taska.core import Env


def main():
    parser = ArgumentParser()
    parser.add_argument("--no-stream-log", action="store_false", dest="stream_log")
    parser.add_argument("--log-file", default="", dest="log_file")
    parser.add_argument("--rm-dir", default="", dest="rm_dir")
    parser.add_argument("--launch-job", default="", dest="launch_job")
    parser.add_argument("--init", "--prepare-root", default="", dest="prepare_root_dir")
    parser.add_argument("--prepare-python", default="", dest="prepare_python_dir")
    parser.add_argument("--prepare-venv", default="", dest="prepare_venv_dir")
    parser.add_argument("--prepare-workspace", default="", dest="prepare_workspace_dir")
    args = parser.parse_args()
    Config.LOG_STREAM = args.stream_log
    if args.log_file:
        Config.LOG_FILE = args.log_file
    Config.init_logger()
    if args.rm_dir:
        return print("Removed:", Env.safe_rm_dir(args.rm_dir), args.rm_dir, flush=True)
    elif args.launch_job:
        return Env.launch_job(Path(args.launch_job))
    if args.prepare_root_dir:
        Env.prepare_root_dir(Path(args.prepare_root_dir))
    if args.prepare_python_dir:
        Env.prepare_python_dir(Path(args.prepare_python_dir))
    if args.prepare_venv_dir:
        Env.prepare_venv_dir(Path(args.prepare_venv_dir))
    if args.prepare_workspace_dir:
        Env.prepare_workspace_dir(Path(args.prepare_workspace_dir))


if __name__ == "__main__":
    main()
