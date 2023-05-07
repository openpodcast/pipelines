CREATE DATABASE IF NOT EXISTS openpodcast;

CREATE TABLE IF NOT EXISTS openpodcast.podcasts (
  account_id INTEGER NOT NULL AUTO_INCREMENT,
  pod_name VARCHAR(2048) NOT NULL,
  PRIMARY KEY (account_id)
);

-- define access keys for podcast sources
CREATE TABLE IF NOT EXISTS podcastSources (
  account_id INTEGER NOT NULL,
  source_name ENUM('spotify','apple', 'anchor'),
  source_podcast_id VARCHAR(64) NOT NULL,
  -- keys are stored in json format and are encrypted by the client
  source_access_keys_encrypted JSON NOT NULL,
  PRIMARY KEY (account_id, source_name)
);

ALTER TABLE podcastSources
ADD CONSTRAINT podcastSources_account_id_fk
FOREIGN KEY (account_id) REFERENCES openpodcast.podcasts(account_id) ON DELETE CASCADE;

GRANT ALL PRIVILEGES ON openpodcast.* TO 'openpodcast'@'%';

INSERT INTO openpodcast.podcasts (account_id, pod_name)
VALUES
(1, 'podcast1'),
(2, 'podcast2'),
(3, 'podcast3');

-- "testvalue" encrypted using key "supersecret" abd base64 encoded is
-- "jA0EBwMCiwr1cCWJid//0j8B7rMbB+DT6lGsQpCerFKIeYNbe3YWcTKsvr+3fwAVwnJvxRbBAILR+9maT6rm56oC740ypydEHXQ7YVgyAIQ="
-- run will fail without proper data but executing of the manager can be tested
INSERT INTO podcastSources (account_id, source_name, source_podcast_id, source_access_keys_encrypted)
VALUES
(1, 'spotify', '1', '{"SPOTIFY_SP_DC": "jA0EBwMCiwr1cCWJid//0j8B7rMbB+DT6lGsQpCerFKIeYNbe3YWcTKsvr+3fwAVwnJvxRbBAILR+9maT6rm56oC740ypydEHXQ7YVgyAIQ=", "SPOTIFY_SP_KEY": "jA0EBwMCiwr1cCWJid//0j8B7rMbB+DT6lGsQpCerFKIeYNbe3YWcTKsvr+3fwAVwnJvxRbBAILR+9maT6rm56oC740ypydEHXQ7YVgyAIQ=", "SPOTIFY_PODCAST_ID": "jA0EBwMCiwr1cCWJid//0j8B7rMbB+DT6lGsQpCerFKIeYNbe3YWcTKsvr+3fwAVwnJvxRbBAILR+9maT6rm56oC740ypydEHXQ7YVgyAIQ=", "OPENPODCAST_API_TOKEN": "jA0EBwMCiwr1cCWJid//0j8B7rMbB+DT6lGsQpCerFKIeYNbe3YWcTKsvr+3fwAVwnJvxRbBAILR+9maT6rm56oC740ypydEHXQ7YVgyAIQ="}'),
(2, 'apple',  '1', '{"APPLE_AUTOMATION_ENDPOINT": "jA0EBwMCiwr1cCWJid//0j8B7rMbB+DT6lGsQpCerFKIeYNbe3YWcTKsvr+3fwAVwnJvxRbBAILR+9maT6rm56oC740ypydEHXQ7YVgyAIQ=", "APPLE_AUTOMATION_BEARER_TOKEN": "jA0EBwMCiwr1cCWJid//0j8B7rMbB+DT6lGsQpCerFKIeYNbe3YWcTKsvr+3fwAVwnJvxRbBAILR+9maT6rm56oC740ypydEHXQ7YVgyAIQ=", "APPLE_PODCAST_ID": "jA0EBwMCiwr1cCWJid//0j8B7rMbB+DT6lGsQpCerFKIeYNbe3YWcTKsvr+3fwAVwnJvxRbBAILR+9maT6rm56oC740ypydEHXQ7YVgyAIQ=", "OPENPODCAST_API_TOKEN": "jA0EBwMCiwr1cCWJid//0j8B7rMbB+DT6lGsQpCerFKIeYNbe3YWcTKsvr+3fwAVwnJvxRbBAILR+9maT6rm56oC740ypydEHXQ7YVgyAIQ="}'),
(3, 'anchor', '1', '{"ANCHOR_WEBSTATION_ID": "jA0ECQMCd59Zr5Spp1P70joBVH4wA5Z51H0z8kTtM6/6L6V6kvfM1iqJNqnMWMpdoGx+eI3KvUL+o70Swqu8kTUA1vuq5yovkkNC", "ANCHOR_PW_S": "jA0ECQMCiq4eqyU0beD70owBXfQcj6xmBhOH1i+os6u0e4NDLVa6SFqCLyna++hyqmpMUVNoLIfrQWiasfIXIqNLMfn0A1bVG7+Pak9YfbK2mBu5V10+ka6jkI6hY+n40knOcro5QXmIhhfPyeYmNHrpYmVB7i9LPoxahRx0GGvatzkaSct22CquM/9Vw+mqiURShkAqoP2wkEjjDQ==", "OPENPODCAST_API_TOKEN": "jA0ECQMCHuVjSUBUvln70ksBKlRpJBvaLovudeM3wMvGvnZ8+mO3lejr0kT/ieb2AykHotJlOKGYPNQyXpy2cEWbG217AWy0pKhpTOQC225mcIJQlH7Ifj9yDW4=" }');