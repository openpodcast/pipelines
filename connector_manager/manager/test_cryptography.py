import unittest
from manager.cryptography import decrypt_json
import json

# "testvalue" encrypted using key "supersecret" abd base64 encoded is
# "jA0EBwMCiwr1cCWJid//0j8B7rMbB+DT6lGsQpCerFKIeYNbe3YWcTKsvr+3fwAVwnJvxRbBAILR+9maT6rm56oC740ypydEHXQ7YVgyAIQ="
passphrase = "supersecret"

original = {"testkey": "testvalue"}
encrypted = {
    "testkey": "jA0EBwMCiwr1cCWJid//0j8B7rMbB+DT6lGsQpCerFKIeYNbe3YWcTKsvr+3fwAVwnJvxRbBAILR+9maT6rm56oC740ypydEHXQ7YVgyAIQ="}

json_original = json.dumps(original)
json_encrypted = json.dumps(encrypted)


class TestDecrypt(unittest.TestCase):
    def test_encrypted_value(self):
        self.assertEqual(decrypt_json(json_encrypted, passphrase), original)

    def test_wrong_passphrase(self):
        self.assertNotEqual(decrypt_json(
            json_encrypted, "wrongpassphrase"), original)

    def test_unecrypted_value_is_untouched(self):
        json = '{"testkey": "testvalue"}'
        self.assertEqual(decrypt_json(json, passphrase)
                         ["testkey"], "testvalue")

    def test_unencrypted_url(self):
        json = '{"APPLE_AUTOMATION_ENDPOINT": "https://apple-automation.somedomain.com/endpoint"}'
        self.assertEqual(decrypt_json(json, passphrase)
                         ["APPLE_AUTOMATION_ENDPOINT"], "https://apple-automation.somedomain.com/endpoint")


if __name__ == "__main__":
    unittest.main()
