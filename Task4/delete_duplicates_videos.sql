DELETE
FROM
  `cosmic-rarity-392606.DG_Task4.videos_raw`
WHERE
  EXISTS (
  SELECT
    1
  FROM
    `cosmic-rarity-392606.DG_Task4.fact_videos` FV
  WHERE
    `cosmic-rarity-392606.DG_Task4.videos_raw`.datahash=FV.datahash);