import gnupg
import base64
import json
import binascii

from loguru import logger

gpg = gnupg.GPG()


def decrypt_json(json_encrypted, key):
    '''
    Decrypts a json string
    The json object keys are plain text, the value is gpg encrypted and base64 encoded
    e.g. {"key1": "base64encodedgpgencryptedvalue1", "key2": "base64encodedgpgencryptedvalue2"}
    the method supports only one level of nesting, deeper nesting is not supported yet
    '''
    dict_encrypted = json.loads(json_encrypted)
    for k in dict_encrypted:
        try:
            encrypted_binary = base64.b64decode(dict_encrypted[k])
            # decrypt the value and remove trailing whitespace which might added by piping stuff around
            decryptedValue = gpg.decrypt(
                encrypted_binary, passphrase=key).data.decode("utf-8").strip()
            # gpg doesn't throw an error if the key is wrong, it just returns an empty string
            # therefore, keep the original value if the decrypted value is empty
            if not decryptedValue and dict_encrypted[k].strip() != "":
                decryptedValue = dict_encrypted[k]
            dict_encrypted[k] = decryptedValue
        except (binascii.Error, ValueError) as e:
            logger.debug(
                f"Error decrypting {k}, assuming plain text and continuing, error: {e}")
    return dict_encrypted
