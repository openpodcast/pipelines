CREATE DATABASE IF NOT EXISTS openpodcast;

CREATE TABLE IF NOT EXISTS openpodcast.updates (
  account_id INTEGER NOT NULL,
  provider VARCHAR(64) NOT NULL,
  endpoint VARCHAR(64) NOT NULL,
  created TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  update_data JSON NOT NULL,
  PRIMARY KEY (created, account_id, endpoint)
);

CREATE TABLE IF NOT EXISTS openpodcast.podcasts (
  account_id INTEGER NOT NULL AUTO_INCREMENT,
  pod_name VARCHAR(2048) NOT NULL,
  PRIMARY KEY (account_id)
);

-- define access keys for podcast sources
CREATE TABLE IF NOT EXISTS podcastSources (
  account_id INTEGER NOT NULL,
  source_name ENUM('spotify','apple', 'anchor', 'podigee'),
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
-- OPENPODCAST API Key for local API is: dummy-cn389ncoiwuencr
-- encoded with supersecret is: jA0ECQMIC/55CSVmUCP/0ksBnq5RMFcGYEf0Ie0rsnHB+SQoZNr7sm35nXlWruibLILn9qIg8fXaHPewqR6VMgXVWkrdZyx/UdwHBaHSXnCXTnnbwI8l1t/3SMs=

INSERT INTO podcastSources (account_id, source_name, source_podcast_id, source_access_keys_encrypted)
VALUES 
-- (1, 'spotify', '1', '{"SPOTIFY_SP_DC": "jA0EBwMCiwr1cCWJid//0j8B7rMbB+DT6lGsQpCerFKIeYNbe3YWcTKsvr+3fwAVwnJvxRbBAILR+9maT6rm56oC740ypydEHXQ7YVgyAIQ=", "SPOTIFY_SP_KEY": "jA0EBwMCiwr1cCWJid//0j8B7rMbB+DT6lGsQpCerFKIeYNbe3YWcTKsvr+3fwAVwnJvxRbBAILR+9maT6rm56oC740ypydEHXQ7YVgyAIQ=", "SPOTIFY_PODCAST_ID": "jA0EBwMCiwr1cCWJid//0j8B7rMbB+DT6lGsQpCerFKIeYNbe3YWcTKsvr+3fwAVwnJvxRbBAILR+9maT6rm56oC740ypydEHXQ7YVgyAIQ=", "OPENPODCAST_API_TOKEN": "jA0EBwMCiwr1cCWJid//0j8B7rMbB+DT6lGsQpCerFKIeYNbe3YWcTKsvr+3fwAVwnJvxRbBAILR+9maT6rm56oC740ypydEHXQ7YVgyAIQ="}'),
-- (2, 'apple',  '1', '{"APPLE_AUTOMATION_ENDPOINT": "jA0EBwMCiwr1cCWJid//0j8B7rMbB+DT6lGsQpCerFKIeYNbe3YWcTKsvr+3fwAVwnJvxRbBAILR+9maT6rm56oC740ypydEHXQ7YVgyAIQ=", "APPLE_AUTOMATION_BEARER_TOKEN": "jA0EBwMCiwr1cCWJid//0j8B7rMbB+DT6lGsQpCerFKIeYNbe3YWcTKsvr+3fwAVwnJvxRbBAILR+9maT6rm56oC740ypydEHXQ7YVgyAIQ=", "APPLE_PODCAST_ID": "jA0EBwMCiwr1cCWJid//0j8B7rMbB+DT6lGsQpCerFKIeYNbe3YWcTKsvr+3fwAVwnJvxRbBAILR+9maT6rm56oC740ypydEHXQ7YVgyAIQ=", "OPENPODCAST_API_TOKEN": "jA0EBwMCiwr1cCWJid//0j8B7rMbB+DT6lGsQpCerFKIeYNbe3YWcTKsvr+3fwAVwnJvxRbBAILR+9maT6rm56oC740ypydEHXQ7YVgyAIQ="}'),
-- anchor: source_podcast_id is the Spotify show URI; encrypted keys contain SPOTIFY_SP_DC + SPOTIFY_SP_KEY
(3, 'anchor', 'spotify:show:3sC4RVsJqwH7Y70utzYWOh', '{"SPOTIFY_SP_DC": "jA0ECQMKhUSPL27XpKr/0sA7AQQLznZAwpHOXwssuzu2p4uDAD7e+27pUiUHYFTKbBqYf7Yz/Fe+d467HwqxODaQbBF1Bc+oeHqfyqirS4js84e3dWJYGMJ4vI8dOP0zCioFmIGu/09Y5LbuZWrjX4+NZNo8IZQXtx7LDwaChm/P7/LJJig0iabbPppRaLUxg7/acfb+k5w9hZYhoCzckCFyWlzDvHCMBhwTFwMymc9santg0xRasWPFAQico33TAX6XXKSmPHGabIz8umNtej1wIfLdiI9wP7Cqh15w+7PpyQAsCOqbnIFTVu7bdWJGeoXoACSs6UfbFq7Fz4OLVU+1/KscgXh2HGcEGwM=",
  "SPOTIFY_SP_KEY": "jA0ECQMKjOmD7oSihZD/0loBCdp5GpUAJPRZG8/1ZNEtlPsBdTGGWVV89BDZHxCXLEC6Rb0J53wpvx6XhurWl+I650pXFRkmYFZHXdeH2DP1D6sYiHi/efzM7gd1EOqe0HGRfFWxOYZAQkM=", "OPENPODCAST_API_TOKEN": "jA0EBwMCiwr1cCWJid//0j8B7rMbB+DT6lGsQpCerFKIeYNbe3YWcTKsvr+3fwAVwnJvxRbBAILR+9maT6rm56oC740ypydEHXQ7YVgyAIQ="}')
-- (3, 'podigee', '51361', '{"PODIGEE_REFRESH_TOKEN": "jA0ECQMI3I//NAxLiRb60mEBwQFWoTv4KmXxPmoI4X5B8XEktOn4PZ0IM/CzsmgeOeKFV63DErompDF/hP9JBSkFRmJchxx7Y4tcuA7jfRw+A85k7u6ZmpRBnVo8TQbH0HISDQMECpzQVHO2cruGxmmT", "OPENPODCAST_API_TOKEN": "jA0ECQMIC/55CSVmUCP/0ksBnq5RMFcGYEf0Ie0rsnHB+SQoZNr7sm35nXlWruibLILn9qIg8fXaHPewqR6VMgXVWkrdZyx/UdwHBaHSXnCXTnnbwI8l1t/3SMs="}')
;

