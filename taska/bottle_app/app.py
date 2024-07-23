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

    @lru_cache_ttl(ttl=3600 * 24 * 3, maxsize=1000, controls=True)
    def get_sign(self, ip):
        return md5(f"{ip}{Config.salt}{Config.pwd}".encode()).hexdigest()

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
        cookie_ok = sign and sign == self.get_sign(client_ip)
        if rule == "/login":
            if request.method == "GET":
                request.environ["cookie_ok"] = cookie_ok
                return True
            else:
                # POST
                self.handle_post_pwd(client_ip, cookie_ok, now)
        s = self.get_params_s(rule)
        if cookie_ok:
            response.set_header("s", s)
            return True
        else:
            # params s auth
            params = request.params
            s_valid = "s" in params and params["s"] == s
            return s_valid

    def handle_post_pwd(self, client_ip, cookie_ok, now):
        pwd = request.forms.get("pwd")
        if not pwd:
            raise HTTPError(401, "No password?")
        if cookie_ok or not Config.pwd:
            # modify pwd
            Config.pwd = pwd
            self.get_sign.cache.clear()
        if pwd == Config.pwd:
            # correct password
            next_url = request.cookies.get("next_url")
            res = response.copy(cls=HTTPResponse)
            res.status = 303
            res.set_cookie("sign", self.get_sign(client_ip))
            if next_url:
                res.delete_cookie("next_url")
            res.body = ""
            res.set_header("Location", next_url or "/")
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
                response.set_cookie("next_url", request.url)
                redirect("/login")
            res = callback(*args, **kwargs)
            return res

        return wrapper


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
/ (index)
    /list/root

/list/{path} (index)
    root, python, venv, workspace, job
/login (input/reset password)
    /login?next=/

/api/launch/{path}
/api/stop/{path}?signal=15
/api/kill/{path}?signal=9
/api/prepare/{path}

/api/upload data {'local_path': 'xxx', 'remote_path': 'xxx'}
/api/delete/{path}

/api/get/json/{path}?s=sign
/api/get/html/{path}
/api/get/bin/{path}

"""


def main():
    app.install(AuthPlugin())
    app.run(server="waitress")


if __name__ == "__main__":
    main()
