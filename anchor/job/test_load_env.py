import os
import tempfile
import unittest

from job.load_env import load_file_or_env


class TestLoadFileOrEnv(unittest.TestCase):
    def setUp(self):
        self.var = "MY_VAR"
        self.default = "default value"
        self.file_content = "file content"
        self.env_value = "environment value"
        self.env_var = f"{self.var}_FILE"

    def test_load_from_env_file(self):
        with tempfile.NamedTemporaryFile(delete=False) as f:
            f.write(self.file_content.encode())
            env_file_path = f.name

        os.environ[self.env_var] = env_file_path
        result = load_file_or_env(self.var)
        self.assertEqual(result, self.file_content)

        os.remove(env_file_path)
        del os.environ[self.env_var]

    def test_load_from_env_var(self):
        os.environ[self.var] = self.env_value
        result = load_file_or_env(self.var)
        self.assertEqual(result, self.env_value)
        del os.environ[self.var]

    def test_load_with_default_value(self):
        result = load_file_or_env(self.var, self.default)
        self.assertEqual(result, self.default)

    def test_load_with_none_default_value(self):
        result = load_file_or_env(self.var, None)
        self.assertIsNone(result)

    def test_load_with_empty_string_default_value(self):
        result = load_file_or_env(self.var, "")
        self.assertEqual(result, "")


if __name__ == "__main__":
    unittest.main()
