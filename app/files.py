import os


def cleanup_file(path: str) -> None:
    try:
        os.remove(path)
    except Exception:
        pass
