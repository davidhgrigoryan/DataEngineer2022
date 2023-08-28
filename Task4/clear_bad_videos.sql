EXPORT DATA
  OPTIONS ( uri = 'gs://dgedu/rawdata/Invalid/videos_*.csv',
    format = 'CSV',
    OVERWRITE = TRUE,
    header = TRUE,
    field_delimiter = ',') AS (
  SELECT
    *
  FROM
    `cosmic-rarity-392606.DG_Task4.videos_raw`
  WHERE
    NOT REGEXP_CONTAINS(url, r"(?i)\b((?:https?://|www\d{0,3}[.]|[a-z0-9.\-]+[.][a-z]{2,4}/)(?:[^\s()<>]+|\(([^\s()<>]+|(\([^\s()<>]+\)))*\))+(?:\(([^\s()<>]+|(\([^\s()<>]+\)))*\)|[^\s`!()\[\]{};:'\'.,<>?«»“”‘’]))") );
    
DELETE
FROM
  `cosmic-rarity-392606.DG_Task4.videos_raw`
WHERE
  NOT REGEXP_CONTAINS(url, r"(?i)\b((?:https?://|www\d{0,3}[.]|[a-z0-9.\-]+[.][a-z]{2,4}/)(?:[^\s()<>]+|\(([^\s()<>]+|(\([^\s()<>]+\)))*\))+(?:\(([^\s()<>]+|(\([^\s()<>]+\)))*\)|[^\s`!()\[\]{};:'\'.,<>?«»“”‘’]))")
;