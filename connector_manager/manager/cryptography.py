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


def encrypt_json(dict_to_encrypt, key):
    '''
    Encrypts a dictionary to a json string
    The json object keys are kept as plain text, the values are gpg encrypted and base64 encoded
    e.g. {"key1": "value1", "key2": "value2"} -> {"key1": "base64encodedgpgencryptedvalue1", "key2": "base64encodedgpgencryptedvalue2"}
    The method supports only one level of nesting, deeper nesting is not supported yet
    '''
    result = {}
    for k, v in dict_to_encrypt.items():
        try:
            # Convert value to string if it's not already
            if not isinstance(v, str):
                v = str(v)
            
            # Encrypt the value with GPG
            encrypted_data = gpg.encrypt(v, recipients=None, symmetric=True, passphrase=key)
            
            # Base64 encode the encrypted data
            if encrypted_data.ok:
                encrypted_base64 = base64.b64encode(encrypted_data.data).decode('utf-8')
                result[k] = encrypted_base64
            else:
                logger.error(f"Failed to encrypt value for key {k}: {encrypted_data.status}")
                # Keep original value if encryption failed
                result[k] = v
        except Exception as e:
            logger.error(f"Error encrypting value for key {k}: {e}")
            # Keep original value if encryption failed
            result[k] = v
    
    return json.dumps(result)
