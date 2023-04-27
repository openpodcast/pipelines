CREATE DATABASE IF NOT EXISTS openpodcast;

CREATE TABLE IF NOT EXISTS openpodcast.podcasts (
  account_id INTEGER NOT NULL AUTO_INCREMENT,
  pod_name VARCHAR(2048) NOT NULL,
  PRIMARY KEY (account_id)
);

-- define access keys for podcast sources
CREATE TABLE IF NOT EXISTS podcastSources (
  account_id INTEGER NOT NULL,
  source_name ENUM('spotify','apple'),
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
(2, 'podcast2');

-- "testvalue" encrypted using key "supersecret" abd base64 encoded is
-- "jA0EBwMCiwr1cCWJid//0j8B7rMbB+DT6lGsQpCerFKIeYNbe3YWcTKsvr+3fwAVwnJvxRbBAILR+9maT6rm56oC740ypydEHXQ7YVgyAIQ="
-- run will fail without proper data but executing of the manager can be tested
INSERT INTO podcastSources (account_id, source_name, source_podcast_id, source_access_keys_encrypted)
VALUES
(1, 'spotify', '1', '{"SPOTIFY_SP_DC": "jA0EBwMCiwr1cCWJid//0j8B7rMbB+DT6lGsQpCerFKIeYNbe3YWcTKsvr+3fwAVwnJvxRbBAILR+9maT6rm56oC740ypydEHXQ7YVgyAIQ=", "SPOTIFY_SP_KEY": "jA0EBwMCiwr1cCWJid//0j8B7rMbB+DT6lGsQpCerFKIeYNbe3YWcTKsvr+3fwAVwnJvxRbBAILR+9maT6rm56oC740ypydEHXQ7YVgyAIQ=", "SPOTIFY_PODCAST_ID": "jA0EBwMCiwr1cCWJid//0j8B7rMbB+DT6lGsQpCerFKIeYNbe3YWcTKsvr+3fwAVwnJvxRbBAILR+9maT6rm56oC740ypydEHXQ7YVgyAIQ=", "OPENPODCAST_API_TOKEN": "jA0EBwMCiwr1cCWJid//0j8B7rMbB+DT6lGsQpCerFKIeYNbe3YWcTKsvr+3fwAVwnJvxRbBAILR+9maT6rm56oC740ypydEHXQ7YVgyAIQ="}'),
(2, 'apple',  '1', '{"APPLE_AUTOMATION_ENDPOINT": "jA0EBwMCiwr1cCWJid//0j8B7rMbB+DT6lGsQpCerFKIeYNbe3YWcTKsvr+3fwAVwnJvxRbBAILR+9maT6rm56oC740ypydEHXQ7YVgyAIQ=", "APPLE_AUTOMATION_BEARER_TOKEN": "jA0EBwMCiwr1cCWJid//0j8B7rMbB+DT6lGsQpCerFKIeYNbe3YWcTKsvr+3fwAVwnJvxRbBAILR+9maT6rm56oC740ypydEHXQ7YVgyAIQ=", "APPLE_PODCAST_ID": "jA0EBwMCiwr1cCWJid//0j8B7rMbB+DT6lGsQpCerFKIeYNbe3YWcTKsvr+3fwAVwnJvxRbBAILR+9maT6rm56oC740ypydEHXQ7YVgyAIQ=", "OPENPODCAST_API_TOKEN": "jA0EBwMCiwr1cCWJid//0j8B7rMbB+DT6lGsQpCerFKIeYNbe3YWcTKsvr+3fwAVwnJvxRbBAILR+9maT6rm56oC740ypydEHXQ7YVgyAIQ="}');
