EXPORT DATA
  OPTIONS ( uri = 'gs://dgedu/rawdata/Invalid/users_*.csv',
    format = 'CSV',
    OVERWRITE = TRUE,
    header = TRUE,
    field_delimiter = ',') AS (
  SELECT
    *
  FROM
    `cosmic-rarity-392606.DG_Task4.users_raw`
  WHERE
    NOT REGEXP_CONTAINS(email, r"@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+")
    OR fname IS NULL
    OR lname IS NULL
    OR email IS NULL);
    
DELETE
FROM
  `cosmic-rarity-392606.DG_Task4.users_raw`
WHERE
  NOT REGEXP_CONTAINS(email, r"@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+")
  OR fname IS NULL
  OR lname IS NULL
  OR email IS NULL; 