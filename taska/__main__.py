from argparse import ArgumentParser
from pathlib import Path

from taska.config import Config
from taska.core import Taska


def main():
    parser = ArgumentParser()
    parser.add_argument("--root", default="", dest="root")
    parser.add_argument(
        "-a", "-app", "--app", "--app-handler", default="default", dest="app_handler"
    )
    parser.add_argument("--no-stream-log", action="store_false", dest="stream_log")
    parser.add_argument("--log-dir", default="{ROOT_DIR}/logs", dest="log_dir")
    parser.add_argument("--rm-dir", default="", dest="rm_dir")
    parser.add_argument("--launch-job", default="", dest="launch_job")
    parser.add_argument("--ignore-default", action="store_true", dest="ignore_default")
    args, extra = parser.parse_known_args()
    if args.root:
        root_path = Path(args.root)
    elif extra:
        assert len(extra) == 1
        root_path = Path(extra[0])
    else:
        raise ValueError("--root is required")
    Config.LOG_STREAM = args.stream_log
    if args.log_dir:
        if args.log_dir == "{ROOT_DIR}/logs":
            args.log_dir = str(root_path / "logs")
        Config.LOG_DIR = args.log_dir
    Config.init_logger()
    if args.rm_dir:
        return print(
            "Removed:", Taska.safe_rm_dir(args.rm_dir), args.rm_dir, flush=True
        )
    elif args.launch_job:
        return Taska.launch_job(Path(args.launch_job))

    if not args.ignore_default:
        Taska.prepare_default_env(root_path)
    # run app
    if args.app_handler == "default":
        return Taska(root_path).run_forever()
    elif args.app_handler == "bottle":
        from taska.bottle_app.app import main

        return main()
    elif args.app_handler == "fastapi":
        raise NotImplementedError("fastapi not implemented yet")

    else:
        raise ValueError("--app-handler is required")


if __name__ == "__main__":
    main()
