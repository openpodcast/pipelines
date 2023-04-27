import gnupg
import base64
import json

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
        encrypted_binary = base64.b64decode(dict_encrypted[k])
        try:
            dict_encrypted[k] = gpg.decrypt(encrypted_binary, passphrase=key).data.decode("utf-8")
        except Exception as e:
            print("Error decrypting element: ", k)
    return dict_encrypted