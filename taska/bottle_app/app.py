import signal
import sys
import time
import typing
from collections import defaultdict
from hashlib import md5
from pathlib import Path
from string import Template
from urllib.parse import quote_plus

from bottle import (
    Bottle,
    HTTPError,
    HTTPResponse,
    redirect,
    request,
    response,
    static_file,
)
from morebuiltins.functools import lru_cache_ttl
from morebuiltins.utils import get_hash, is_running, read_size, read_time, ttime
from psutil import Process

from ..config import Config as MConfig
from ..core import JobDir, PythonDir, Taska, VenvDir, WorkspaceDir
from .console_template import console_template

app = Bottle()
keepalive_timeout = 60
keepalives = {}
# import sys

# sys.path.append("../../")
# from taska.core import DirBase
logger = MConfig.init_logger()


class Config:
    pwd = ""
    salt = md5(Path(__file__).read_bytes()).hexdigest()
    root_path = Path.cwd()
    # file size limit
    max_file_size = 1024 * 100
    console_template = Template(console_template)


class AuthPlugin(object):
    # avoid tries too many times
    blacklist: typing.Dict[str, int] = defaultdict(lambda: 0)
    cookie_max_age = 7 * 86400

    def get_sign(self, now: int, ip: str):
        _hash = md5(
            f"{now+self.cookie_max_age}{ip}{Config.salt}{Config.pwd}".encode()
        ).hexdigest()
        return f"{now+self.cookie_max_age}{_hash}"

    @lru_cache_ttl(ttl=300, maxsize=100, controls=True)
    def get_params_s(self, rule: str):
        return md5(f"{rule}{Config.salt}".encode()).hexdigest()

    def check_blacklist(self, client_ip, now):
        b = self.blacklist[client_ip]
        if b:
            if b > now:
                self.blacklist[client_ip] = b + 5
                timeleft = self.blacklist[client_ip] - now
                raise HTTPError(429, f"Too many tries, retry at {timeleft}s later.")
            else:
                self.blacklist.pop(client_ip, None)

    @lru_cache_ttl(ttl=3600 * 1, maxsize=1000, controls=True)
    def check_cookie(self, sign, client_ip, now):
        if sign:
            try:
                then = int(sign[:10])
                if now > then:
                    return False
                else:
                    return sign == self.get_sign(then - self.cookie_max_age, client_ip)
            except ValueError:
                return False
        else:
            return False

    def is_valid(self, rule):
        client_ip = request.environ.get("HTTP_X_FORWARDED_FOR") or request.environ.get(
            "REMOTE_ADDR"
        )
        if not client_ip:
            raise HTTPError(401, "No client ip")
        now = int(time.time())
        if self.check_blacklist(client_ip, now):
            return True
        sign = request.cookies.get("sign")
        cookie_ok = self.check_cookie(sign, client_ip, now)
        if rule == "/login":
            if request.method == "GET":
                request.environ["cookie_ok"] = cookie_ok
                return True
            else:
                # POST
                self.handle_post_pwd(rule, client_ip, cookie_ok, now)
        s = self.get_params_s(rule)
        if cookie_ok:
            response.set_header("s", s)
            return True
        else:
            # params s auth
            params = request.params
            s_valid = "s" in params and params["s"] == s
            return s_valid

    def handle_post_pwd(self, rule, client_ip, cookie_ok, now):
        pwd = request.forms.get("pwd")
        if not pwd:
            raise HTTPError(401, "No password?")
        if cookie_ok or not Config.pwd:
            # modify pwd
            Config.pwd = pwd
            self.check_cookie.cache.clear()
        if pwd == Config.pwd:
            # correct password
            from_url = request.cookies.get("from_url")
            res = response.copy(cls=HTTPResponse)
            res.status = 303
            res.set_cookie(
                "sign",
                self.get_sign(now, client_ip),
                path="/",
                max_age=self.cookie_max_age * 0.95,
            )
            if from_url and rule != "/login":
                res.delete_cookie("from_url")
            res.body = ""
            res.set_header("Location", from_url or "/")
            raise res
        else:
            # wrong password
            self.blacklist[client_ip] = now + 5
            raise HTTPError(401, "Invalid password")

    def apply(self, callback, context):
        rule = context["rule"]

        def wrapper(*args, **kwargs):
            valid = self.is_valid(rule)
            if not valid:
                if rule != "/login":
                    response.set_cookie("from_url", request.url, path="/", max_age=3600)
                redirect("/login")
            request.environ["auth_ok"] = 1
            res = callback(*args, **kwargs)
            return res

        return wrapper


@app.get("/")
def index():
    # 1. console
    # 2. view
    return r"""
<style>
button{
    list-style-type: none;
    display: inline-block;
    vertical-align: middle;
    zoom: 1;
    border: 1px solid #d4d4d4;
    font-weight: bold;
    font-family: "Helvetica Neue Light", "Helvetica Neue", Helvetica, Arial, "Lucida Grande", sans-serif;
    margin: 20% 50px 20% 50px;
    text-decoration: none;
    text-align: center;
    border-radius: 60%;
    box-shadow: inset 0px 1px 1px rgba(255, 255, 255, 0.5), 0px 1px 2px rgba(0, 0, 0, 0.2);
    width: 300px;
    line-height: 30%;
    height: 30%;
    padding: 0px;
    border-width: 4px;
    font-size: 3em;
    background: -webkit-linear-gradient(top, #00b5e5, #008db2);
    background-color: #00a1cb;
    border-color: #007998;
    color: white;
    text-shadow: 0 -1px 1px rgba(0, 40, 50, 0.35);
    cursor: pointer;
}
button:hover{
    background: -webkit-linear-gradient(top, #dcdcdc, #189086);
}
</style>
<body style='width: 50%;height: 100%;margin: 0 auto;'>
<a href='/view//'><button>Files</button></a>
<a href='/console'><button>Console</button></a>
</body>
"""


@app.get("/favicon.ico")
def favico():
    response.body = '<svg xmlns="http://www.w3.org/2000/svg"  viewBox="0 0 128 128" width="64px" height="64px"><path fill="#F7F7FB" d="M64 9A55 55 0 1 0 64 119A55 55 0 1 0 64 9Z"/><path fill="#DEDFE6" d="M64,9C33.6,9,9,33.6,9,64s24.6,55,55,55s55-24.6,55-55S94.4,9,64,9z M64,105.2c-22.8,0-41.2-18.5-41.2-41.2S41.2,22.8,64,22.8s41.2,18.5,41.2,41.2S86.8,105.2,64,105.2z"/><path fill="#D8D7D5" d="M64 59.4A4.6 4.6 0 1 0 64 68.6A4.6 4.6 0 1 0 64 59.4Z"/><path fill="#464C55" d="M64,122C32,122,6,96,6,64S32,6,64,6s58,26,58,58S96,122,64,122z M64,12c-28.7,0-52,23.3-52,52s23.3,52,52,52s52-23.3,52-52S92.7,12,64,12z"/><path fill="#464C55" d="M64.1,67.1c-0.8,0-1.5-0.3-2.1-0.9s-0.9-1.3-0.9-2.1L61,36.5c0-1.7,1.3-3,3-3l0,0c1.7,0,3,1.3,3,3l0.1,24.6L82.3,61l0,0c1.6,0,3,1.3,3,3c0,1.7-1.3,3-3,3L64.1,67.1L64.1,67.1z"/><path fill="#464C55" d="M64,71.6c-4.2,0-7.6-3.4-7.6-7.6s3.4-7.6,7.6-7.6s7.6,3.4,7.6,7.6S68.2,71.6,64,71.6z M64,62.4c-0.9,0-1.6,0.7-1.6,1.6c0,0.9,0.7,1.6,1.6,1.6c0.9,0,1.6-0.7,1.6-1.6S64.9,62.4,64,62.4z"/></svg>'
    _md5 = get_hash(response.body)
    response.headers["Cache-Control"] = "public, max-age=%s" % 3600 * 24
    response.add_header("ETag", _md5)
    response.add_header("Content-Type", "image/svg+xml")
    return response


@app.get("/login")
@app.post("/login")
def login():
    if request.method == "GET":
        if Config.pwd and not request.environ.get("cookie_ok"):
            placeholder = "Input the password"
        else:
            placeholder = "Reset the password"
        return r"""
    <form style="width: 100%;height: 100%;" action="/login" method="post">
    <input autofocus style="text-align: center;font-size: 5em;width: 100%;height: 100%;" type="password" name="pwd" placeholder="{placeholder}">
    </form>""".format(placeholder=placeholder)


@app.get("/init/<dir_type>")
def init(dir_type):
    referer = request.query["referer"]
    name = request.query.get("name")
    root = Config.root_path
    target_dir = root.joinpath(referer).resolve()
    if not (target_dir.exists() and target_dir.is_relative_to(root)):
        return "path not found"
    if dir_type == "requirements":
        return VenvDir.ensure_pip_install(target_dir)
    c = {i.__name__: i for i in [JobDir, PythonDir, VenvDir, WorkspaceDir]}[dir_type]
    if c is PythonDir:
        python = request.query.get("python", "")
        c.prepare_dir(target_dir, name, python=python)
    else:
        c.prepare_dir(target_dir, name)
    referer = referer or "/"
    return redirect(f"/view/{referer}")


def get_list_html(path: Path):
    html = "<a style='color: black' href='/'>Home</a> - "
    parts = path.relative_to(Config.root_path.parent).parts
    for index, part in enumerate(parts):
        if index == 0:
            html += f" <a style='color:blue' href='/view//'>{part}</a> /"
        else:
            p = "/".join(parts[1 : index + 1])
            html += f" <a style='color:blue' href='/view/{p}'>{part}</a> /"
    html = html.rstrip("/")
    path_arg = "/".join(parts[1:])
    if JobDir.is_valid(path) or JobDir.is_valid(path.parent):
        html += "<hr>"
        kill_html = ""
        for pid_dir in [path.parent, path]:
            try:
                if pid_dir.is_dir():
                    pid = int(pid_dir.joinpath("pid.txt").read_bytes())
                    if is_running(pid):
                        kill_html = f" | <a style='color:red' href='/console?kill={pid}&signal=15'>Kill - {pid}</a>"
            except (FileNotFoundError, ValueError):
                pass
        html += f"<a style='color:red' href='/launch/{path_arg}?timeout=2'>Launch Job</a> | <a style='color:#009879' href='/console'>Console</a>{kill_html}"
    elif path.is_dir():
        if path == Config.root_path:
            "create python dir"
            html += f" | <form style='color:red' method='get' action='/init/{PythonDir.__name__}'><input placeholder='dir name' name='name'> <input placeholder='python_path' name='python'><input style='display:none' name='referer' value='{path_arg}' name='python'><input type='submit' value='Create PythonDir'></form>"
        elif PythonDir.is_valid(path):
            "create venv dir"
            html += f" | <form style='color:red' method='get' action='/init/{VenvDir.__name__}'><input placeholder='dir name' name='name'><input style='display:none' name='referer' value='{path_arg}'><input type='submit' value='Create VenvDir'></form>"
        elif VenvDir.is_valid(path):
            "fresh requirements"
            html += f" | <a style='color:red' target='_blank' href='/init/requirements?referer={path_arg}'>PIP install</a>"
        elif VenvDir.is_valid(path.parent) and path.name == "workspaces":
            "create workspace dir"
            html += f" | <form style='color:red' method='get' action='/init/{WorkspaceDir.__name__}'><input placeholder='dir name' name='name'><input style='display:none' name='referer' value='{path_arg}'><input type='submit' value='Create WorkspaceDir'></form>"
        elif WorkspaceDir.is_valid(path.parent) and path.name == "jobs":
            "create job dir"
            html += f" | <form style='color:red' method='get' action='/init/{JobDir.__name__}'><input placeholder='dir name' name='name'><input style='display:none' name='referer' value='{path_arg}'><input type='submit' value='Create JobDir'></form>"
    elif path.is_file() and path.suffix == ".py" and WorkspaceDir.is_valid(path.parent):
        html += f" | <form style='color:red' method='get' action='/init/{JobDir.__name__}'><input placeholder='dir name' name='name'><input style='display:none' name='referer' value='{path_arg}'><input type='submit' value='Create Job'></form>"
    html += "<hr>"
    text_arg = ""
    file_name_arg = ""
    old_color = "#696969"
    new_color = "#00c308"
    now = time.time()
    if path.is_dir():
        path_list = sorted(
            path.iterdir(), key=lambda i: f"-{i.name}" if i.is_dir() else i.name
        )
        for _path in path_list:
            p = _path.relative_to(Config.root_path).as_posix()
            mtime = _path.stat().st_mtime
            if _path.is_dir():
                color = "darkorange"
                icon = "&#128194;"
                size = " - "
                stat_color = old_color
            else:
                color = "black"
                icon = "&#128196;"
                size = read_size(_path.stat().st_size, 1, shorten=True)
                if now - mtime < 5 * 60:
                    stat_color = new_color
                else:
                    stat_color = old_color
            time_stat = f"{ttime(mtime)}({read_time(now-mtime, shorten=True):->8})"
            stat = f"<span style='color:{stat_color};width:260px;display: inline-block;font-size: 0.8em'> | {time_stat} | {size}</span>"
            file_url = f"{request.url.rstrip('/')}/{quote_plus(_path.name)}"
            dir_disabled = " disabled" if _path.is_dir() else ""
            rename_html = f"<form action='/rename' method='get' style='display: inline;'><input style='display:none' name='old_path' value='{path_arg}/{_path.name}'><input type='text' name='name' value='{_path.name}'><input type='submit' value='Rename'></form> | "
            html += f"{rename_html}<button onclick='delete_path(`{file_url}?action=delete`)'>Delete</button> | <a href='{file_url}?action=download'><button{dir_disabled}>Download</button></a> | <a href='{file_url}?action=view'><button{dir_disabled}>View</button></a> {stat} <a style='color:{color}' href='/view/{p}'>{icon} {_path.name}</a>"
            # add rename form
            html += "<br>"
    else:
        file_name_arg = path.name
        p = path.relative_to(Config.root_path).as_posix()
        mtime = path.stat().st_mtime
        if now - mtime < 5 * 60:
            stat_color = new_color
        else:
            stat_color = old_color
        _path = path
        time_stat = f"{ttime(mtime)}({read_time(now-mtime, shorten=True):->8})"
        stat = f"<span style='color:{stat_color};font-size: 0.8em;width:260px;display: inline-block;'> | {read_size(path.stat().st_size, 1)}|{time_stat}</span>"
        html += f"<button onclick='delete_path(`{request.url}?action=delete`)'>Delete</button> | <a href='{request.url}?action=download'><button>Download</button></a> | <a href='{request.url}?action=view'><button>View</button></a> {stat} <br>"
        if path.stat().st_size < Config.max_file_size:
            text_arg = path.read_bytes().decode("utf-8", "replace")
    max_text_tip = f"preview text-only file_size < {read_size(Config.max_file_size)}"
    html += """<hr><form action="/upload" method="post" enctype="multipart/form-data" id="upload_form">
<input type="hidden" name="path" value="{path_arg}">
File Name:
<input type="text" name="file_name" value="{file_name_arg}"> or <input type="file" name="upload_file"><br>
<textarea placeholder="{max_text_tip}" title="{max_text_tip}" id="text" name="text" style='width:100%;height:50%;border: groove;padding: 2em;font-size: 1.5em;text-wrap: pretty;'>{text_arg}</textarea>
<br>
<input style="font-size: 1.5em;" type="submit" value="Upload <Ctrl+Enter>" /></form>
""".format(
        path_arg=path_arg,
        file_name_arg=file_name_arg,
        text_arg=text_arg,
        max_text_tip=max_text_tip,
    )
    html += r"""<script>
document.addEventListener('DOMContentLoaded', function() {
    var upload_form = document.getElementById('upload_form');
    upload_form.addEventListener('keydown', function(event) {
        if (event.ctrlKey && event.key === 'Enter') {
            event.preventDefault();
            document.getElementById('upload_form').submit();
        }
    });
});
</script>"""
    delete_code = r"""<script>function delete_path(url){
    var isConfirmed = confirm('Are you sure you want to delete this item?');
    if (isConfirmed) {
        window.location.href = url;
    }
    }</script>"""
    return f"<body style='width:80%;margin: 0 auto;'>{html}{delete_code}</body>"


@app.get("/rename")
def rename():
    old_path = request.query["old_path"].lstrip("/")
    path = Config.root_path.joinpath(old_path).resolve()
    if path.exists() and path.is_relative_to(Config.root_path):
        path.rename(path.with_name(request.query["name"]))
    redirect(f"/view/{'/'.join(old_path.split('/')[:-1]) or '/'}")


@app.route("/launch/<path:path>")
def launch(path):
    root = Config.root_path
    _path: Path = root.joinpath(path).resolve()
    if not (_path.exists() and _path.is_relative_to(root)):
        return "path not found"
    timeout = int(request.query.get("timeout", 0))
    job_dir = Taska.launch_job(_path, timeout)
    parts = job_dir.relative_to(Config.root_path.parent).parts
    path_arg = "/".join(parts[1:])
    return redirect(f"/view/{path_arg}")


@app.get("/view")
@app.get("/view/")
def redirect_view_root():
    redirect("/view//")


@app.get("/view/<path:path>")
def list_dir(path):
    if path == "/":
        path = ""
    root = Config.root_path
    real_path: Path = root.joinpath(path).resolve()
    action = request.query.get("action")
    if not (real_path.exists() and real_path.is_relative_to(root)):
        return "path not found"
    elif action == "delete":
        if not real_path.parent.is_relative_to(root):
            return "path not found"
        if real_path.is_dir():
            Taska.safe_rm_dir(real_path)
        else:
            real_path.unlink()
        back = "/".join(request.path.split("/")[:-1])
        if back == "/view":
            return redirect("/view//")
        redirect(back)
    elif action == "download":
        if not real_path.exists():
            return HTTPError(400, "path not found")
        elif not real_path.is_relative_to(root):
            return HTTPError(400, "bad path")
        elif real_path.is_dir():
            return HTTPError(400, "not support download dir")
        else:
            content_type = "application/octet-stream"
            file_content = static_file(
                real_path.as_posix(), real_path.parent.as_posix(), content_type
            )
            response.headers["Content-Disposition"] = (
                f'attachment; filename="{real_path.name}"'
            )
            response.body = file_content
            return response
    elif action == "view":
        grep = request.query.get("grep")
        if real_path.is_file():
            if grep:
                encoding = request.query.get("encoding") or "utf-8"
                with open(real_path, "r", encoding=encoding) as f:
                    lines = []
                    for line in f:
                        if grep in line:
                            lines.append(line)
                    result = "".join(lines)
                    return f"<pre>{result}</pre>"
            else:
                return real_path.read_bytes()
        else:
            return "not a file"
    elif "tail" in request.query:
        return handle_tail(real_path, get_hash((time.time(), real_path.as_posix())))

    else:
        return get_list_html(real_path)


def handle_tail(path: Path, event_id):
    if not path.is_file():
        raise ValueError("not a file")
    tail = int(request.query["tail"])
    encoding = request.query.get("encoding", "utf-8")
    interval = int(request.query.get("interval", 1))
    with open(path, "r", encoding=encoding) as f:
        if tail:
            for index, line in enumerate(f):
                pass
            min_index = index + 1 - tail
            f.seek(0)
            yield "<pre style='font-size: 1.5em;'>"
            for index, line in enumerate(f):
                if index >= min_index:
                    yield line
            yield "</pre>"
        else:
            keepalives[event_id] = int(time.time() + keepalive_timeout)
            yield (
                "<script> (function () { setInterval(() => document.readyState !== 'complete' && fetch('/keepalive?e=%s', { method: 'HEAD' }), %s); })()</script>"
                % (event_id, (keepalive_timeout * 1000 // 2))
            )
            yield "<pre style='font-size: 1.5em;'>"
            # tail -F
            # end of file
            f.seek(path.stat().st_size)
            while True:
                line = f.readline()
                if line:
                    yield line
                else:
                    if time.time() > keepalives.get(event_id, 0):
                        keepalives.pop(event_id, None)
                        break
                    elif path.stat().st_size < f.tell():
                        f.seek(0)
                    time.sleep(interval)
            yield "</pre>"


@app.route("/keepalive", method="HEAD")
def keepalive():
    if request.query.get("d"):
        keepalives.pop(request.query["e"], None)
    else:
        now = int(time.time())
        keepalives[request.query["e"]] = now + keepalive_timeout
    for k, v in list(keepalives.items()):
        if now > v:
            keepalives.pop(k, None)


@app.post("/upload")
def upload():
    file_name = request.forms.get("file_name")
    upload_file = request.files.get("upload_file")
    text = request.forms.get("text")
    path = request.forms.get("path")
    target_dir = Config.root_path.joinpath(request.forms.get("path"))
    if target_dir.is_file() and target_dir.name == file_name:
        target_dir = target_dir.parent
    if not target_dir.is_dir() or not target_dir.is_relative_to(Config.root_path):
        return HTTPError(400, "bad path")
    if upload_file.raw_filename:
        file_name = file_name or upload_file.raw_filename
        upload_file.save(
            target_dir.joinpath(file_name).resolve().as_posix(),
            overwrite=True,
        )
        upload_file.file.close()
    else:
        if not file_name:
            return HTTPError(400, "file_name must be set if text is not null")
        target_file = target_dir.joinpath(file_name).resolve()
        if not target_file.is_relative_to(Config.root_path):
            return HTTPError(400, "bad path")
        if file_name.endswith("/"):
            target_file.mkdir(parents=True, exist_ok=True)
        else:
            target_file.parent.mkdir(parents=True, exist_ok=True)
            target_file.write_text(text, encoding="utf-8", newline="")
    redirect(f"/view/{path}")


@app.get("/console")
def console():
    root = Config.root_path
    kill = request.query.get("kill")
    if kill:
        pid = int(kill)
        if root.joinpath(f"pids/{pid}").is_file():
            signal = int(request.query.get("signal") or 2)
            proc = Process(pid)
            if proc.is_running():
                if signal == 9:
                    proc.kill()
                elif signal == 15:
                    proc.terminate()
                elif signal == 2:
                    if sys.platform == "win32":
                        proc.kill()
                    else:
                        proc.send_signal(2)
                else:
                    raise ValueError("bad signal")
                proc.wait(5)
        redirect(request.headers.get("Referer") or "/console")
    pids_dir = root.joinpath("pids")
    pids = []
    for pid_path in root.rglob("pid.txt"):
        pid = None
        try:
            pid = int(pid_path.read_bytes())
        except FileNotFoundError:
            pass
        finally:
            if pid:
                if is_running(pid):
                    pids.append(pid)
                else:
                    pids_dir.joinpath(str(pid)).unlink(missing_ok=True)
                    pid_path.unlink(missing_ok=True)
    m_file = root.joinpath("max_workers")
    if m_file.is_file():
        max_workers = m_file.read_text().strip() or "-"
    else:
        max_workers = "-"
    items: dict = Taska.get_pids_info(pids)
    Taska.LATEST_PROC_CACHE.update(items)
    # [{'pid': 10916, 'status': 'running', 'job_dir': 'default/venv1/workspaces/workspace1/jobs/job1', 'start_at': '2024-08-05 21:36:35', 'elapsed': '19 secs', 'memory': '17 MB'}]
    th_list = [
        f"<th>{k}</th>"
        for k in [
            f"*/{max_workers} - <a style='color: #ffffff' href='/'>Home</a>",
            "pid",
            "status",
            "start_at",
            "end_at",
            "elapsed",
            "memory",
            "job_dir",
            "kill-2",
            "kill-15",
            "kill-9",
        ]
    ]
    tr_list = []
    rows = sorted(
        Taska.LATEST_PROC_CACHE.items(),
        key=lambda x: x[1]["start_at"],
        reverse=True,
    )
    cache_length = Taska.CACHE_LENGTH
    for row_id, (pid, item) in enumerate(rows, 1):
        if pid in items:
            tr_list.append(proc_info_to_tr(item, row_id, pid))
        elif row_id < cache_length:
            if item["status"] != "dead":
                cache_item = Taska.LATEST_PROC_CACHE[pid]
                cache_item["status"] = "dead"
                cache_item["start_at"], cache_item["end_at"], cache_item["elapsed"] = (
                    Taska.get_end_at(item)
                )
                item = cache_item
            tr_list.append(proc_info_to_tr(item, row_id, pid))
        else:
            Taska.LATEST_PROC_CACHE.pop(pid, None)
    html = Config.console_template.substitute(
        th_list="\n".join(th_list), tr_list="\n".join(tr_list)
    )
    return html


def proc_info_to_tr(item, row_id, pid):
    grep = "%s%s" % (quote_plus('"pid": '), item["pid"])
    href = f'<a target="_blank" href="/view/{item["job_dir"]}">{item["job_dir"]}</a>; <a target="_blank" href="/view/{item["job_dir"]}/result.jsonl?action=view&grep={grep}">result</a>'
    if item["status"] == "running":
        buttons = f"""<td><button onclick='redirect("?kill={pid}&signal=2")'>kill</button></td><td><button onclick='redirect("?kill={pid}&signal=15")'>kill</button></td><td><button onclick='redirect("?kill={pid}&signal=9")' style='color:red'>kill</button></td>"""
    else:
        buttons = """<td>-</td><td>-</td><td>-</td>"""
    return f"""<tr class="{item['status']}"><td>{row_id}</td><td>{item['pid']}</td><td>{item['status']}</td><td>{item['start_at']}</td><td>{item.get('end_at', '-')}</td><td>{item['elapsed']}</td><td>{item['memory']}</td><td>{href}</td>{buttons}</tr>"""


def handle_signal(sig, b):
    logger.warn(f"signal received {sig}, close app")
    sys.exit()


def main(root_path, host="127.0.0.1", port=8021, debug=False):
    Config.root_path = Path(root_path).resolve()
    app.install(AuthPlugin())
    logger.warning(
        f"start server: root_path: {Config.root_path}, debug={debug}, http://{host}:{port}"
    )
    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)
    app.run(server="waitress", host=host, port=port, debug=debug)
