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

INSERT INTO podcastSources (account_id, source_name, source_podcast_id, source_access_keys_encrypted)
VALUES
(1, 'spotify', '1', '{"access_token": "1", "refresh_token": "1"}'),
(2, 'apple',  '1', '{"access_token": "2", "refresh_token": "2"}');
