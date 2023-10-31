import os


def load_env(var, default=None):
    """
    Load environment variable or return default
    (even when the variable is set to an empty string)
    """
    var = os.environ.get(var, default)
    if var is None or var == "":
        return default
    return var


def load_file_or_env(var, default=None):
    """
    Load environment variable from file or environment variable
    """
    env_file_path = os.environ.get(f"{var}_FILE", None)
    if env_file_path and os.path.isfile(env_file_path):
        with open(env_file_path, "r", encoding="utf-8") as env_file:
            return env_file.read().strip()
    # Fallback to environment variable if file does not exist
    return load_env(var, default)
