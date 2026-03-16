import os


def load_env(var, default=None):
    """Load env var or return default (even when set to empty string)."""
    val = os.environ.get(var, default)
    if val is None or val == "":
        return default
    return val


def load_file_or_env(var, default=None):
    """Load from a _FILE path or fall back to the env var directly."""
    env_file_path = os.environ.get(f"{var}_FILE", None)
    if env_file_path and os.path.isfile(env_file_path):
        with open(env_file_path, "r", encoding="utf-8") as f:
            return f.read().strip()
    return load_env(var, default)
