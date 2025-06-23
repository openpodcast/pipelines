import unittest
from manager.cryptography import decrypt_json, encrypt_json
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
        # apparently the url can be base64-encoded and the gpg fails without an error msg, just an empty string
        json = '{"APPLE_AUTOMATION_ENDPOINT": "https://apple-automation.somedomain.com/endpoint"}'
        self.assertEqual(decrypt_json(json, passphrase)
                         ["APPLE_AUTOMATION_ENDPOINT"], "https://apple-automation.somedomain.com/endpoint")


class TestEncrypt(unittest.TestCase):
    def test_encrypt_decrypt_roundtrip(self):
        # Test that encrypting and then decrypting returns the original value
        test_data = {"key1": "value1", "key2": "value2"}
        encrypted_json = encrypt_json(test_data, passphrase)
        decrypted_data = decrypt_json(encrypted_json, passphrase)
        self.assertEqual(decrypted_data, test_data)

    def test_encrypt_with_different_keys(self):
        # Test that encrypting the same data with different keys produces different results
        test_data = {"key1": "value1"}
        encrypted1 = encrypt_json(test_data, "key1")
        encrypted2 = encrypt_json(test_data, "key2")
        self.assertNotEqual(encrypted1, encrypted2)

    def test_encrypt_non_string_values(self):
        # Test that non-string values are properly handled
        test_data = {"int": 123, "float": 3.14, "bool": True, "none": None}
        encrypted_json = encrypt_json(test_data, passphrase)
        decrypted_data = decrypt_json(encrypted_json, passphrase)

        # Convert expected values to strings since encrypt_json converts non-string values to strings
        expected = {k: str(v) if v is not None else "None" for k, v in test_data.items()}
        self.assertEqual(decrypted_data, expected)

    def test_wrong_passphrase_decrypt(self):
        # Test that decrypting with wrong passphrase fails
        test_data = {"secret": "confidential"}
        encrypted_json = encrypt_json(test_data, passphrase)
        decrypted_data = decrypt_json(encrypted_json, "wrongpassphrase")
        self.assertNotEqual(decrypted_data["secret"], test_data["secret"])


if __name__ == "__main__":
    unittest.main()
