#!/usr/bin/env python

"""External engine provider example for lichess.org"""

import argparse
import concurrent.futures
import contextlib
import logging
import multiprocessing
import os
import requests
from requests.adapters import HTTPAdapter, Retry
import secrets
import subprocess
import sys
import time
import json
from types import SimpleNamespace
import threading
import psutil

TOTAL_MEM_MiB = int(psutil.virtual_memory().total / 1024 / 1024)
MAX_HASH = int(TOTAL_MEM_MiB/2)
MAX_HASH_GB = int(MAX_HASH/1024)
MAX_THREADS = multiprocessing.cpu_count()  # int(multiprocessing.cpu_count()/2)

DEFAULT_HASH = int(TOTAL_MEM_MiB/4)
DEFAULT_THREADS = int(MAX_THREADS/2)
DEFAULT_KEEP_ALIVE = 5*60

_LOG_LEVEL_MAP = {
        "critical": logging.CRITICAL,
        "error": logging.CRITICAL,
        "warning": logging.WARNING,
        "info": logging.INFO,
        "debug": logging.DEBUG,
        "notset": logging.NOTSET,
        }

class CustomFormatter(logging.Formatter):

    grey = "\x1b[38;20m"
    green = "\x1b[32;20m"
    yellow = "\x1b[33;20m"
    red = "\x1b[31;20m"
    bold_red = "\x1b[31;1m"
    reset = "\x1b[0m"
    # %(name)s
    format = "%(asctime)s %(levelname)s %(message)s (%(filename)s:%(lineno)d)"

    FORMATS = {
        logging.DEBUG: grey + format + reset,
        logging.INFO: green + format + reset,
        logging.WARNING: yellow + format + reset,
        logging.ERROR: red + format + reset,
        logging.CRITICAL: bold_red + format + reset
    }

    def format(self, record):
        log_fmt = self.FORMATS.get(record.levelno)
        formatter = logging.Formatter(log_fmt)
        return formatter.format(record)


def ok(res):
    try:
        print('Got HTTP res', res, 'res.text', res.text)
        res.raise_for_status()
    except requests.exceptions.HTTPError as e:
        logging.exception('Got HTTP Error %s', res.text)
        # logging.error("Response: %s", res.text)
        raise
    return res


class XtEngProvider:
    def __init__(self, args, eng_cfg, executor):
        self.args = args
        self.eng_cfg = eng_cfg
        self.executor = executor
        self.engine = Engine(eng_cfg)
        self.name = hasattr(eng_cfg, 'name') and eng_cfg.name or self.engine.name
        self.keep_alive = hasattr(eng_cfg, 'keep_alive') and eng_cfg.keep_alive or args.keep_alive
        self.http = requests.Session()
        self.http.headers["Authorization"] = f"Bearer {args.token}"
        self.secret = self.register_engine(args, self.http, self.engine)

        retries = Retry(total=5, backoff_factor=0.2, status_forcelist=[500, 502, 503, 504])
        self.http.mount('https://', HTTPAdapter(max_retries=retries))


    def register_engine(self, args, http, engine):
        res = ok(http.get(f"{args.lichess}/api/external-engine"))

        secret = hasattr(self.eng_cfg, 'provider_secret') and self.eng_cfg.provider_secret or secrets.token_urlsafe(32)

        variants = {
            "chess",
            "antichess",
            "atomic",
            "crazyhouse",
            "horde",
            "kingofthehill",
            "racingkings",
            "3check",
        }

        registration = {
            "name": self.name,
            "maxThreads": hasattr(self.eng_cfg, "max_threads") and self.eng_cfg.max_threads or args.max_threads,
            # lila's maxHash is limited to 512, but local engine can use more
            "maxHash": 512, # args.max_hash
            "variants": [variant for variant in engine.supported_variants or ["chess"] if variant in variants],
            "providerSecret": secret,
        }
        logging.debug('registration %s', registration)

        for engine in res.json():
            if engine["name"] == self.name:
                logging.info("Updating engine %s", engine["id"])
                ok(http.put(f"{args.lichess}/api/external-engine/{engine['id']}", json=registration))
                break
        else:
            logging.info("Registering new engine %s", self.name)
            ok(http.post(f"{args.lichess}/api/external-engine", json=registration))

        return secret

    def proc_work(self):
        logging.info('processing work %s', self.name)
        args = self.args
        backoff = 1
        last_future = None
        while True:
            try:
                res = ok(self.http.post(f"{args.broker}/api/external-engine/work", json={"providerSecret": self.secret}, timeout=12))
                if res.status_code == 204:  # No Content
                    logging.debug('No work yet. %s', self.name)
                if res.status_code != 200:
                    if self.engine.alive and self.engine.idle_time() > self.keep_alive:
                        logging.info("Terminating idle engine %s", self.name)
                        self.engine.terminate()
                    continue
                job = res.json()
            except requests.exceptions.RequestException as err:
                # if len(res.text):
                logging.error("Error while trying to acquire work: %s (%s)", err, self.name)
                backoff = min(backoff * 1.5, 10)
                time.sleep(backoff)
                continue
            else:
                backoff = 1

            try:
                self.engine.stop()
            except EOFError:
                pass

            if last_future:
                logging.debug('Waiting for last job done (%s)', self.name)
                last_future.result()  # waiting for last handle_job done, i.e. posted result to server

            if not self.engine.alive:
                self.engine = Engine(self.eng_cfg)

            job_started = threading.Event()
            last_future = self.executor.submit(self.handle_job, args, self.engine, job, job_started)
            job_started.wait()


    def handle_job(self, args, engine, job, job_started):
        try:
            logging.info("Handling job %s (%s)", job["id"], self.name)
            with engine.analyse(job, job_started) as analysis_stream:
                ok(requests.post(f"{args.broker}/api/external-engine/work/{job['id']}", data=analysis_stream))
        except requests.exceptions.ConnectionError:
            logging.info("Connection closed while streaming analysis (%s)", self.name)
        except requests.exceptions.RequestException as err:
            logging.exception("Error while submitting work (%s)", self.name)
            time.sleep(5)
        except EOFError:
            logging.exception("Engine died (%s)", self.name)
            time.sleep(5)
        finally:
            job_started.set()


def main(args):
    max_workers = 0
    if args.engine:
        max_workers = 1
    if args.config:
        max_workers = len(args.config.engines)

    executor = concurrent.futures.ThreadPoolExecutor(max_workers=max_workers*2)
    providers = {}
    if args.engine:
        p = XtEngProvider(args, args, executor)
        providers[p.name] = p

    if args.config:
        for cfg in args.config.engines:
            p = XtEngProvider(args, cfg, executor)
            providers[p.name] = p

    logging.info('providers %s', providers)
    futures = {}
    pcnt = 0
    for pname, p in providers.items():
        pcnt += 1
        futures[pname] = executor.submit(p.proc_work)
    while True:
        dcnt = 0
        futures_left = {}
        for fname, f in futures.items():
            try:
                logging.debug("provider %s done: %s", fname, f.result(5))
                dcnt +=1
            except TimeoutError:
                futures_left[fname] = f
                pass
        if len(futures_left.items()) == 0:
            break
        futures = futures_left


def kill_process_and_children(pid: int, sig: int = 15):
    try:
        proc = psutil.Process(pid)
    except psutil.NoSuchProcess as e:
        logging.warning('No such process %d', pid)
        return

    for child_process in proc.children(recursive=True):
        child_process.send_signal(sig)

    proc.send_signal(sig)


class Engine:
    def __init__(self, cfg):
        logging.info('Starting engine %s', cfg.engine)
        self.process = subprocess.Popen(cfg.engine, shell=True, stdin=subprocess.PIPE, stdout=subprocess.PIPE, bufsize=1, universal_newlines=True)
        self.name = None
        self.cfg = cfg
        self.session_id = None
        self.hash = 64
        self.threads = None
        self.multi_pv = None
        self.uci_variant = None
        self.supported_variants = []
        self.last_used = time.monotonic()
        self.alive = True
        self.stop_lock = threading.Lock()

        self.uci()
        self.setoption("UCI_AnalyseMode", "true")
        self.setoption("UCI_Chess960", "true")
        self.setoption("UCI_ShowWDL", "false")
        self.setoption("Hash", self.hash)
        if hasattr(cfg, 'threads'):
            self.setoption("Threads", cfg.threads)
        else:
            self.setoption("Threads", DEFAULT_THREADS)

        if hasattr(cfg, 'setoption'):
            if  isinstance(cfg.setoption, list):
                options = cfg.setoption
            else:
                options = vars(cfg.setoption).items()
            for name, value in options:
                self.setoption(name, value)
        logging.info('Engine %s started', cfg.engine)

    def idle_time(self):
        return time.monotonic() - self.last_used

    def terminate(self):
        # self.process.terminate()  # only terminates invoking shell
        kill_process_and_children(self.process.pid)  # Popen Shell=True, need to kill child process manully
        self.process.wait()
        self.alive = False

    def send(self, command):
        self.last_used = time.monotonic()
        logging.info("%d <cmd> %s (%s)", self.process.pid, command, self.name)
        self.process.stdin.write(command + "\n")
        self.process.stdin.flush()

    def recv(self):
        while True:
            self.last_used = time.monotonic()
            line = self.process.stdout.readline()
            if line == "":
                self.alive = False
                raise EOFError("Empty line received, abort!")

            line = line.rstrip()
            if not line:
                continue

            if 'bestmove' in line:
                logging.info("%d <resp> %s (%s)", self.process.pid, line, self.name)
            else:
                logging.debug("%d <resp> %s (%s)", self.process.pid, line, self.name)

            command_and_params = line.split(None, 1)

            if len(command_and_params) == 1:
                return command_and_params[0], ""
            else:
                return command_and_params

    def uci(self):
        self.send("uci")
        while True:
            command, args = self.recv()
            if command == "option":
                name = None
                args = args.split()
                while args:
                    arg = args.pop(0)
                    if arg == "name":
                        name = args.pop(0)
                    elif name == "UCI_Variant" and arg == "var":
                        self.supported_variants.append(args.pop(0))
            elif command == "uciok":
                break
            elif command == "id":
                k, v = args.split(None, 1)
                if k == "name":  # engine name from uci
                    self.name = "[L] " + v

        if self.supported_variants:
            logging.info("Supported variants: %s", ", ".join(self.supported_variants))

    def isready(self):
        self.send("isready")
        while True:
            line, _ = self.recv()
            if line == "readyok":
                break

    def setoption(self, name, value):
        if value is False:
            value = "false"
        elif value is True:
            value = "true"
        self.send(f"setoption name {name} value {value}")

    @contextlib.contextmanager
    def analyse(self, job, job_started):
        work = job["work"]

        if work["sessionId"] != self.session_id:
            self.session_id = work["sessionId"]
            self.send("ucinewgame")
            self.isready()

        options_changed = False
        if self.threads != work["threads"]:
            self.setoption("Threads", work["threads"])
            self.threads = work["threads"]
            options_changed = True

        # Lichess configurable Hash size 512 is too small
        # if self.hash != work["hash"]:
        #     self.setoption("Hash", work["hash"])
        #     self.hash = work["hash"]
        #     options_changed = True

        running_hash = self.cfg.hash if hasattr(self.cfg, 'hash') else DEFAULT_HASH
        if self.hash != running_hash:
            self.setoption("Hash", running_hash)
            self.hash = running_hash

        if self.multi_pv != work["multiPv"]:
            self.setoption("MultiPV", work["multiPv"])
            self.multi_pv = work["multiPv"]
            options_changed = True
        if self.uci_variant != work["variant"]:
            self.setoption("UCI_Variant", work["variant"])
            self.uci_variant = work["variant"]
            options_changed = True
        if options_changed:
            self.isready()

        self.send(f"position fen {work['initialFen']} moves {' '.join(work['moves'])}")

        for key in ["movetime", "depth", "nodes"]:
            if key in work:
                self.send(f"go {key} {work[key]}")
                break

        job_started.set()  # Signal self.proc_work to fetch next work

        def stream():
            while True:
                command, params = self.recv()
                if command == "bestmove":
                    break
                elif command == "info":
                    if "score" in params:
                        yield (command + " " + params + "\n").encode("utf-8")
                else:
                    logging.warning("Unexpected engine command: %s", command)

        analysis = stream()
        try:
            yield analysis
        finally:
            self.stop()
            for _ in analysis:
                pass

        self.last_used = time.monotonic()

    def stop(self):
        if self.alive:
            with self.stop_lock:
                self.send("stop")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__, fromfile_prefix_chars='@')
    parser.add_argument("--name", help="Engine name to register")
    parser.add_argument("--engine", help="Shell command to launch UCI engine", required=False)
    # parser.add_argument("--config", default=os.path.join(os.path.dirname(os.path.realpath(__file__)), 'config.json'), help="Configs of UCI engines", required=False)
    parser.add_argument("--config", help="Configs of UCI engines", required=False)
    parser.add_argument("--setoption", nargs=2, action="append", default=[], metavar=("NAME", "VALUE"), help="Set a custom UCI option")
    parser.add_argument("--lichess", default="https://lichess.org", help="Defaults to https://lichess.org")
    parser.add_argument("--broker", default="https://engine.lichess.ovh", help="Defaults to https://engine.lichess.ovh")
    parser.add_argument("--token", default=os.environ.get("LICHESS_API_TOKEN"), help="API token with engine:read and engine:write scopes")
    parser.add_argument("--provider-secret", default=os.environ.get("PROVIDER_SECRET"), help="Optional fixed provider secret")
    parser.add_argument("--max-threads", type=int, default=MAX_THREADS, help="Maximum number of available threads")
    parser.add_argument("--max-hash", type=int, default=MAX_HASH, help="Maximum hash table size in MiB")
    parser.add_argument("--keep-alive", type=int, default=DEFAULT_KEEP_ALIVE, help="Number of seconds to keep an idle/unused engine process around")
    parser.add_argument("--log-level", default="debug", choices=_LOG_LEVEL_MAP.keys(), help="Logging verbosity")

    try:
        import argcomplete
    except ImportError:
        pass
    else:
        argcomplete.autocomplete(parser)

    args = parser.parse_args()

    # logging.basicConfig(level=_LOG_LEVEL_MAP[args.log_level],
    #                     format='%(asctime)s,%(msecs)03d %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s',datefmt='%Y-%m-%dT%H:%M:%S')
    rootLogger = logging.getLogger()
    rootLogger.setLevel(_LOG_LEVEL_MAP[args.log_level])
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    ch.setFormatter(CustomFormatter())
    rootLogger.addHandler(ch)

    logging.debug(args)
    logging.info('Total Mem %d MiB (%d GiB)', TOTAL_MEM_MiB, TOTAL_MEM_MiB/1024)

    if not args.engine and not args.config:
        print(f"One of --engine and --config must be specified.")
        sys.exit(128)

    if args.config:
        with open(args.config) as f:
            args.config = json.load(f, object_hook=lambda d: SimpleNamespace(**d))

    if not args.token:
        print(f"Need LICHESS_API_TOKEN environment variable from {args.lichess}/account/oauth/token/create?scopes[]=engine:read&scopes[]=engine:write")
        sys.exit(128)

    main(args)
