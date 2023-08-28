MERGE
  `cosmic-rarity-392606.DG_Task4.fact_users` AS output
USING
  (
  SELECT
    src.ID AS PSEUDO_ID,
    src.*
  FROM
    cosmic-rarity-392606.DG_Task4.users_raw AS src

  UNION ALL
  
  SELECT
    NULL AS PSEUDO_ID,
    dup.*
  FROM
    `cosmic-rarity-392606.DG_Task4.users_raw` AS dup
  INNER JOIN
    `cosmic-rarity-392606.DG_Task4.fact_users` AS trg
  ON
    dup.ID = trg.USER_ID
  WHERE
    trg.effective_end = '9999-01-01'
    AND trg.EFFECTIVE_START < TIMESTAMP_SECONDS(dup.updated_timestamp) ) AS input
ON
  input.PSEUDO_ID = output.USER_ID 
  WHEN NOT MATCHED THEN INSERT (USER_KEY, USER_ID, FNAME, LNAME, EMAIL, COUNTRY, SUBSRIPTION, EFFECTIVE_START, EFFECTIVE_END, ACTIVE) 
  VALUES ( GENERATE_UUID(),input.ID,input.fname,input.lname,input.email,input.country,CAST(input.subscription AS integer),TIMESTAMP_SECONDS(input.updated_timestamp),'9999-01-01',TRUE ) -------------------------------
  WHEN MATCHED
  AND output.effective_end = '9999-01-01'
  AND output.effective_start < TIMESTAMP_SECONDS(input.updated_timestamp) THEN
UPDATE
SET
  ACTIVE = FALSE,
  effective_end = TIMESTAMP_SECONDS(input.updated_timestamp) 

  WHEN MATCHED
  AND output.effective_end <> '9999-01-01'
  AND output.effective_start < TIMESTAMP_SECONDS(input.updated_timestamp)
  AND output.effective_end > TIMESTAMP_SECONDS(input.updated_timestamp)
  AND output.ACTIVE=FALSE THEN
UPDATE
SET
  effective_end = TIMESTAMP_SECONDS(input.updated_timestamp);
  
  update `cosmic-rarity-392606.DG_Task4.fact_users` set datahash=SHA256(upper(concat(FNAME, LNAME, EMAIL, COUNTRY, SUBSRIPTION))) where datahash is NULL;