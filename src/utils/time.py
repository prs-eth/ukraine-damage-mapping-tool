import time


def timeit(func):
    def wrapper(*args, **kwargs):
        start = time.time()
        result = func(*args, **kwargs)
        end = time.time()
        print(f"{func.__name__} executed in {print_sec(end - start)}")
        return result

    return wrapper


def print_sec(sec: int) -> str:
    """Transforms seconds into hours, minutes and seconds."""
    if sec < 60:
        return f"{sec:.2f}s"
    elif sec < 3600:
        return f"{sec // 60:.0f}min and {sec % 60:.0f}s"
    else:
        h = sec // 3600
        m = (sec % 3600) // 60
        s = sec % 60
        return f"{h:.0f}h, {m:.0f}min and {s:.0f}s"
