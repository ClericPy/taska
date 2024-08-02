import shutil
import time
import typing
from collections import defaultdict
from hashlib import md5
from pathlib import Path
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
from morebuiltins.utils import read_size, ttime

from ..core import Taska, JobDir

app = Bottle()

# import sys

# sys.path.append("../../")
# from taska.core import DirBase


class Config:
    pwd = ""
    salt = md5(Path(__file__).read_bytes()).hexdigest()
    root_path = Path.cwd()
    # file size limit
    max_file_size = 1024 * 16


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


@app.error(404)
def error404(error):
    return HTTPResponse(status=303, headers={"Location": "/"})


@app.get("/")
def home():
    return ""


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


"""
/login (input/reset password)
    /login
/ (index)
    /view/root

/view/{path} (index)
    root, python, venv, workspace, job
    /view/demo_path/default/venv1/workspace1/job1
        meta.json
        ...
    /api/upload/{file}
/api/prepare/{path}
    root, python, venv, workspace, job

/api/launch/{job_path}
    /api/start/{job_path}
/api/update/{job_path}?enable=1
    /api/update/{job_path}?enable=0
/api/stop/{job_path}?signal=15
/api/kill/{job_path}?signal=9

/api/delete/{path}

/file/{path}?s={sign}&type=json
    type: html, bin, text
        import mimetypes
        print(mimetypes.guess_type('1.json'))
        print(mimetypes.guess_type(r'1.py'))
        print(mimetypes.guess_type(r'1.mp4'))
        ('application/json', None)
        ('text/x-python', None)
        ('video/mp4', None)
"""


def get_list_html(path: Path):
    html = ""
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
        html += f" | <a style='color:red' href='/launch/{path_arg}'>Launch Job</a>"
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
            else:
                color = "black"
                icon = "&#128196;"
                size = read_size(_path.stat().st_size, 1, shorten=True)
            if now - mtime < 5 * 60:
                stat_color = new_color
            else:
                stat_color = old_color
            stat = f"<span style='color:{stat_color};width:200;display: inline-block;font-size: 0.8em'> | {ttime(mtime)} | {size}</span>"
            html += f"<button onclick='delete_path(`{request.url}?delete={quote_plus(_path.name)}`)'>Delete</button> | <a href='{request.url}?download={quote_plus(_path.name)}'><button>Download</button></a> {stat} <a style='color:{color}' href='/view/{p}'>{icon} {_path.name}</a><br>"
    else:
        file_name_arg = path.name
        p = path.relative_to(Config.root_path).as_posix()
        mtime = path.stat().st_mtime
        if now - mtime < 5 * 60:
            stat_color = new_color
        else:
            stat_color = old_color
        stat = f"<span style='color:{stat_color};font-size: 0.8em;width:200;display: inline-block;'> | {read_size(path.stat().st_size, 1)}|{ttime(mtime)}</span>"
        html += f"<a style='color:black' href='/view/{p}'>{p}</a> {stat}<br>"
        if path.stat().st_size < Config.max_file_size:
            text_arg = path.read_bytes().decode("utf-8", "replace")
    html += """<hr><form action="/upload" method="post" enctype="multipart/form-data">
<input type="hidden" name="path" value="{path_arg}">
File Name:
<input type="text" name="file_name" value="{file_name_arg}"> or <input type="file" name="upload_file"><br>
<textarea id="text" name="text" style='width:60%;height:50%;border: groove;padding: 2em;font-size: 1.5em;text-wrap: pretty;'>{text_arg}</textarea>
<br>
<input type="submit" value="Upload" /></form>""".format(
        path_arg=path_arg, file_name_arg=file_name_arg, text_arg=text_arg
    )
    delete_code = r"""<script>function delete_path(url){
    var isConfirmed = confirm('Are you sure you want to delete this item?');
    if (isConfirmed) {
        window.location.href = url;
    }
    }</script>"""
    return f"<body style='width:80%;margin: 0 auto;'>{html}{delete_code}</body>"


@app.route("/launch/<path:path>")
def launch(path):
    root = Config.root_path
    _path: Path = root.joinpath(path).resolve()
    if not (_path.exists() and _path.is_relative_to(root)):
        return "path not found"
    Taska.launch_job(_path)
    return redirect(f"/view/{path}")


@app.get("/view/<path:path>")
def list_dir(path):
    if path == "/":
        path = ""
    root = Config.root_path
    path: Path = root.joinpath(path).resolve()
    if not (path.exists() and path.is_relative_to(root)):
        return "path not found"
    delete = request.query.get("delete")
    if delete:
        target = path.joinpath(delete).resolve()
        if not target.parent.is_relative_to(root):
            return "path not found"
        if target.is_dir():
            shutil.rmtree(target)
        else:
            target.unlink()
        redirect(request.path)
    download = request.query.get("download")
    if download:
        target = path.joinpath(download).resolve()
        if not target.exists():
            return HTTPError(400, "path not found")
        elif not target.is_relative_to(root):
            return HTTPError(400, "bad path")
        elif target.is_dir():
            return HTTPError(400, "not support download dir")
        else:
            content_type = "application/octet-stream"
            file_content = static_file(
                target.as_posix(), target.parent.as_posix(), content_type
            )
            response.headers["Content-Disposition"] = (
                f'attachment; filename="{target.name}"'
            )
            response.body = file_content
            return response
    return get_list_html(path)


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
    elif text:
        if not file_name:
            return HTTPError(400, "file_name must be set if text is not null")
        target_file = target_dir.joinpath(file_name).resolve()
        if not target_file.is_relative_to(Config.root_path):
            return HTTPError(400, "bad path")
        target_file.parent.mkdir(parents=True, exist_ok=True)
        target_file.write_text(text, encoding="utf-8", newline="")
    else:
        return HTTPError(400, "text or file must be set")
    redirect(f"/view/{path}")


def main(root_path="../../demo_path"):
    # app.install(AuthPlugin())
    Config.root_path = Path(root_path).resolve()
    app.run(server="waitress", reload=True, debug=True)


if __name__ == "__main__":
    main()
# demo_path\default\venv1\workspaces\workspace1\jobs\test
