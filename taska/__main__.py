from argparse import ArgumentParser
from pathlib import Path

from taska.config import Config
from taska.core import Taska


def start_bottle_app(root_path, host="127.0.0.1", port=8021, debug=False):
    from threading import Thread

    from taska.bottle_app.app import main

    taska = Taska()
    thread = Thread(target=taska.run_forever)
    thread.start()
    try:
        return main(root_path, host=host, port=port, debug=debug)
    finally:
        Taska.SHUTDOWN = True
        thread.join()


def main():
    parser = ArgumentParser()
    parser.add_argument("--root", default="", dest="root")
    parser.add_argument("--no-stream-log", action="store_false", dest="stream_log")
    parser.add_argument("--rm-dir", default="", dest="rm_dir")
    parser.add_argument("--launch-job", default="", dest="launch_job")
    parser.add_argument("--ignore-default", action="store_true", dest="ignore_default")
    parser.add_argument(
        "-a",
        "-app",
        "--app",
        "--app-handler",
        default="bottle",
        dest="app_handler",
        help="default/bottle",
    )
    parser.add_argument("--host", default="127.0.0.1", dest="host")
    parser.add_argument("--port", default=8021, type=int, dest="port")
    parser.add_argument("--debug", action="store_true", dest="debug")
    args, extra = parser.parse_known_args()
    if args.root:
        root_path = Path(args.root).resolve()
    elif extra:
        assert len(extra) == 1
        root_path = Path(extra[0]).resolve()
    else:
        raise ValueError("--root is required")
    Taska.ROOT_PATH = root_path
    Config.LOG_STREAM = args.stream_log
    Config.LOG_DIR = root_path.joinpath("logs")
    Config.init_logger()
    if args.rm_dir:
        return print(
            "Removed:", Taska.safe_rm_dir(args.rm_dir), args.rm_dir, flush=True
        )
    elif args.launch_job:
        return Taska.launch_job(Path(args.launch_job))
    else:
        if not args.ignore_default:
            Taska.prepare_default_env(root_path)
        # run app
        if args.app_handler == "default":
            return Taska().run_forever()
        elif args.app_handler == "bottle":
            return start_bottle_app(root_path, args.host, args.port, args.debug)
        elif args.app_handler == "fastapi":
            raise NotImplementedError("fastapi not implemented yet")

        else:
            raise ValueError("--app-handler is required")


if __name__ == "__main__":
    main()
