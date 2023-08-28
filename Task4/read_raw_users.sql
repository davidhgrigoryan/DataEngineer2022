TRUNCATE TABLE
  `cosmic-rarity-392606.DG_Task4.users_raw`;
LOAD DATA OVERWRITE
  `cosmic-rarity-392606.DG_Task4.users_raw`
FROM FILES ( format = 'CSV',
    uris = ['gs://dgedu/rawdata/Users/*.csv']);