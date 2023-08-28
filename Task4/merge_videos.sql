MERGE
  `cosmic-rarity-392606.DG_Task4.fact_videos` AS output
USING
  (
  SELECT
    src.ID AS PSEUDO_ID,
    src.*
  FROM
    `cosmic-rarity-392606.DG_Task4.videos_raw` AS src

  UNION ALL
  
  SELECT
    NULL AS PSEUDO_ID,
    dup.*
  FROM
    `cosmic-rarity-392606.DG_Task4.videos_raw` AS dup
  INNER JOIN
    `cosmic-rarity-392606.DG_Task4.fact_videos` AS trg
  ON
    dup.ID = trg.VIDEO_ID
  WHERE
    trg.effective_end = '9999-01-01'
    AND trg.EFFECTIVE_START < TIMESTAMP_SECONDS(dup.updated_timestamp) ) AS input
ON
  input.PSEUDO_ID = output.VIDEO_ID 
  WHEN NOT MATCHED THEN INSERT (VIDEO_KEY, VIDEO_ID, NAME, URL, CREATOR_ID, PRIVATE, EFFECTIVE_START, EFFECTIVE_END, ACTIVE) 
  VALUES ( GENERATE_UUID(),input.ID,input.name,input.url,input.creator_id,input.private,TIMESTAMP_SECONDS(input.updated_timestamp),'9999-01-01',TRUE ) 
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
  
  update `cosmic-rarity-392606.DG_Task4.fact_videos` set datahash=SHA256(upper(concat(NAME, URL, CREATOR_ID, PRIVATE))) WHERE datahash is NULL;