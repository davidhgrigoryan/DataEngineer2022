alter table `cosmic-rarity-392606.DG_Task4.users_raw`
add column datahash bytes;

update `cosmic-rarity-392606.DG_Task4.users_raw` set datahash=SHA256(upper(concat(FNAME, LNAME, EMAIL, COUNTRY, SUBSCRIPTION))) WHERE 42=42;

alter table `cosmic-rarity-392606.DG_Task4.videos_raw`
add column datahash bytes;

update `cosmic-rarity-392606.DG_Task4.videos_raw` set datahash=SHA256(upper(concat(NAME, URL, CREATOR_ID, PRIVATE))) WHERE 42=42;