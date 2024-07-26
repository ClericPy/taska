import time
import typing
from collections import defaultdict
from hashlib import md5
from pathlib import Path

from bottle import Bottle, HTTPError, HTTPResponse, redirect, request, response
from morebuiltins.functools import lru_cache_ttl

app = Bottle()


class Config:
    pwd = ""
    salt = md5(Path(__file__).read_bytes()).hexdigest()


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
    return "ok"


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
    /list/root

/list/{path} (index)
    root, python, venv, workspace, job
    /list/demo_path/default/venv1/workspace1/job1
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


@app.get("/root/<path:path>")
def list_dir(path):
    root = app.root_path
    path = root.joinpath(path)
    if not str(path.is_dir() and path.is_relative_to(root)):
        raise HTTPError(404, "Not found")
    return [i.name if i.is_file() else f"{i.name}/" for i in path.iterdir()]


def main(root_path="../../demo_path"):
    app.install(AuthPlugin())
    app.root_path = Path(root_path)
    app.run(server="waitress")


if __name__ == "__main__":
    main()
# demo_path\default\venv1\workspaces\workspace1\jobs\test
