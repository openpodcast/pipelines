from manager.load_env import load_env
from manager.load_env import load_file_or_env
import mysql.connector
from manager.cryptography import decrypt_json
import os
import subprocess

print("Initializing worker environment")

CONNECTORS_PATH = load_env("CONNECTORS_PATH", "./../")

MYSQL_HOST = load_env("MYSQL_HOST", "localhost")
MYSQL_PORT = load_env("MYSQL_PORT", 3306)
MYSQL_USER = load_env("MYSQL_USER", "root")
MYSQL_PASSWORD = load_file_or_env("MYSQL_PASSWORD")
MYSQL_DATABASE = load_env("MYSQL_DATABASE", "openpodcast_auth")

OPENPODCAST_ENCRYPTION_KEY = load_file_or_env("OPENPODCAST_ENCRYPTION_KEY")
if not OPENPODCAST_ENCRYPTION_KEY:
    print("No OPENPODCAST_ENCRYPTION_KEY found")
    exit(1)    

# try to connect to mysql or exit otherwise
try:
    db = mysql.connector.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        passwd=MYSQL_PASSWORD,
        database=MYSQL_DATABASE,
    )
except Exception as e:
    print("Error connecting to mysql: ", e)
    exit(1)

print("Fetching all podcast tasks from database...")

sql = """
  SELECT account_id, source_name, source_podcast_id, source_access_keys_encrypted, pod_name
  FROM podcastSources JOIN openpodcast.podcasts USING (account_id)
"""
cursor = db.cursor()
cursor.execute(sql)
results = cursor.fetchall()

for result in results:
  podcast_id = result[0] # the account id of the podcast
  source_name = result[1] # e.g. spotify or apple
  source_podcast_id = result[2] # e.g. the podcast id of spotify or apple
  source_access_keys = decrypt_json(result[3], OPENPODCAST_ENCRYPTION_KEY) # all keys that are needed to access the source
  pod_name = result[4] # the name of the podcast
  print("Starting fetcher for", pod_name, podcast_id , "for", source_name, "using podcast_id", source_podcast_id)

  # parent path of fetcher/connector
  cwd=os.path.join(CONNECTORS_PATH, source_name)

  # run an external process, switch to right fetcher depending on source_name, and set env variables from source_access_keys  
  subprocess.run(["python","-m","job"], cwd=cwd, env={**os.environ, **source_access_keys})

