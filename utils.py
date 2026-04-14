from datetime import datetime


def now():
    return datetime.now().strftime("%H:%M:%S")


def log(msg):
    print(f"[{now()}] {msg}")


def log_green(msg):
    print(f"\033[92m[{now()}] {msg}\033[0m")


def log_red(msg):
    print(f"\033[91m[{now()}] {msg}\033[0m")


def log_yellow(msg):
    print(f"\033[93m[{now()}] {msg}\033[0m")


def log_cyan(msg):
    print(f"\033[96m[{now()}] {msg}\033[0m")
