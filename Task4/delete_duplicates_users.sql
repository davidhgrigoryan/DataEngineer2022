DELETE
FROM
  `cosmic-rarity-392606.DG_Task4.users_raw`
WHERE
  EXISTS (
  SELECT
    1
  FROM
    `cosmic-rarity-392606.DG_Task4.fact_users` FU
  WHERE
    `cosmic-rarity-392606.DG_Task4.users_raw`.datahash=FU.datahash);