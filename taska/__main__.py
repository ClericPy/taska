from argparse import ArgumentParser
from pathlib import Path

from taska.config import Config
from taska.core import Taska


def main():
    parser = ArgumentParser()
    parser.add_argument("--root", default="", dest="root")
    parser.add_argument("--no-stream-log", action="store_false", dest="stream_log")
    parser.add_argument("--log-file", default="", dest="log_file")
    parser.add_argument("--rm-dir", default="", dest="rm_dir")
    parser.add_argument("--launch-job", default="", dest="launch_job")
    args = parser.parse_args()
    Config.LOG_STREAM = args.stream_log
    if args.log_file:
        Config.LOG_FILE = args.log_file
    Config.init_logger()
    if args.rm_dir:
        return print(
            "Removed:", Taska.safe_rm_dir(args.rm_dir), args.rm_dir, flush=True
        )
    elif args.launch_job:
        return Taska.launch_job(Path(args.launch_job))
    if not args.root:
        raise ValueError("--root is required")
    root_path = Path(args.root)
    if not root_path.exists():
        print("Created:", root_path, flush=True)
        root_path.mkdir(parents=True, exist_ok=True)
        Taska.prepare_default_env(root_path)
    # run app


if __name__ == "__main__":
    main()
