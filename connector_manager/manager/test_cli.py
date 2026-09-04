import subprocess
import sys


def test_help_lists_available_flags_without_initializing_manager():
    result = subprocess.run(
        [sys.executable, "-m", "manager", "--help"],
        capture_output=True,
        check=False,
        text=True,
    )

    assert result.returncode == 0
    assert "usage: python -m manager" in result.stdout
    assert "--interactive" in result.stdout
    assert "--skip-repetition-check" in result.stdout
    assert "Initializing worker environment" not in result.stdout
    assert result.stderr == ""
