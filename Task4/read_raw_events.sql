TRUNCATE TABLE
  `cosmic-rarity-392606.DG_Task4.events_raw`;
LOAD DATA OVERWRITE
  `cosmic-rarity-392606.DG_Task4.events_raw`
FROM FILES ( format = 'JSON',
    uris = ['gs://dgedu/rawdata/Events/*.jsonl'])