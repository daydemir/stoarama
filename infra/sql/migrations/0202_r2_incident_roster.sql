-- Permanent incident identity is independent of plan mode or receipt state.
-- Offline controller descriptors pin RAW source IDs, never cleanup item IDs.
-- Canonical descriptors are immutable VALUES; no runtime roster writer exists.
DO $$ BEGIN
 IF NOT pg_try_advisory_xact_lock(7343226011173) THEN
  RAISE EXCEPTION 'R2 retirement apply is active; incident migration refused';
 END IF;
END $$;
SELECT pg_advisory_xact_lock(7343226011103);
SELECT pg_advisory_xact_lock(7343226011101);
CREATE VIEW nas_cleanup_incident_roster AS
 SELECT kind,source_id,bucket,object_key,raw_source_ids FROM (VALUES
 ('raw'::text,296368::bigint,''::text,''::text,ARRAY[296368]::bigint[]),
 ('raw'::text,296406::bigint,''::text,''::text,ARRAY[296406]::bigint[]),
 ('raw'::text,296453::bigint,''::text,''::text,ARRAY[296453]::bigint[]),
 ('raw'::text,296637::bigint,''::text,''::text,ARRAY[296637]::bigint[]),
 ('raw'::text,296684::bigint,''::text,''::text,ARRAY[296684]::bigint[]),
 ('raw'::text,296727::bigint,''::text,''::text,ARRAY[296727]::bigint[]),
 ('raw'::text,296769::bigint,''::text,''::text,ARRAY[296769]::bigint[]),
 ('raw'::text,296844::bigint,''::text,''::text,ARRAY[296844]::bigint[]),
 ('raw'::text,296891::bigint,''::text,''::text,ARRAY[296891]::bigint[]),
 ('raw'::text,296940::bigint,''::text,''::text,ARRAY[296940]::bigint[]),
 ('raw'::text,296985::bigint,''::text,''::text,ARRAY[296985]::bigint[]),
 ('raw'::text,297022::bigint,''::text,''::text,ARRAY[297022]::bigint[]),
 ('raw'::text,297109::bigint,''::text,''::text,ARRAY[297109]::bigint[]),
 ('raw'::text,297152::bigint,''::text,''::text,ARRAY[297152]::bigint[]),
 ('raw'::text,297204::bigint,''::text,''::text,ARRAY[297204]::bigint[]),
 ('raw'::text,297248::bigint,''::text,''::text,ARRAY[297248]::bigint[]),
 ('raw'::text,297292::bigint,''::text,''::text,ARRAY[297292]::bigint[]),
 ('raw'::text,297335::bigint,''::text,''::text,ARRAY[297335]::bigint[]),
 ('raw'::text,297379::bigint,''::text,''::text,ARRAY[297379]::bigint[]),
 ('raw'::text,297419::bigint,''::text,''::text,ARRAY[297419]::bigint[]),
 ('raw'::text,297469::bigint,''::text,''::text,ARRAY[297469]::bigint[]),
 ('raw'::text,297516::bigint,''::text,''::text,ARRAY[297516]::bigint[]),
 ('raw'::text,297559::bigint,''::text,''::text,ARRAY[297559]::bigint[]),
 ('raw'::text,297602::bigint,''::text,''::text,ARRAY[297602]::bigint[]),
 ('raw'::text,297638::bigint,''::text,''::text,ARRAY[297638]::bigint[]),
 ('raw'::text,297684::bigint,''::text,''::text,ARRAY[297684]::bigint[]),
 ('raw'::text,297734::bigint,''::text,''::text,ARRAY[297734]::bigint[]),
 ('raw'::text,297859::bigint,''::text,''::text,ARRAY[297859]::bigint[]),
 ('raw'::text,1389879::bigint,'stoarama'::text,'managed/acct-47/100018_Europe_Czechia_Vrchlabi_Vrchlabi_-_Nmst_T.G._Masaryka/September/15-Tuesday/100018_Vrchlabi_-_Nmst_T.G._Masaryka_2026_September_W3_Tuesday_hour_110107.mp4'::text,ARRAY[1389879]::bigint[]),
 ('raw'::text,811569::bigint,'stoarama'::text,'managed/acct-47/100018_Europe_Czechia_Vrchlabi_Vrchlabi_-_Nmst_T.G._Masaryka/August/26-Wednesday/100018_Vrchlabi_-_Nmst_T.G._Masaryka_2026_August_W4_Wednesday_hour_165711.mp4'::text,ARRAY[811569]::bigint[]),
 ('raw'::text,1243693::bigint,'stoarama'::text,'managed/acct-47/100018_Europe_Czechia_Vrchlabi_Vrchlabi_-_Nmst_T.G._Masaryka/September/08-Tuesday/100018_Vrchlabi_-_Nmst_T.G._Masaryka_2026_September_W2_Tuesday_hour_092829.mp4'::text,ARRAY[1243693]::bigint[]),
 ('raw'::text,803836::bigint,'stoarama'::text,'managed/acct-47/100018_Europe_Czechia_Vrchlabi_Vrchlabi_-_Nmst_T.G._Masaryka/August/26-Wednesday/100018_Vrchlabi_-_Nmst_T.G._Masaryka_2026_August_W4_Wednesday_hour_113111.mp4'::text,ARRAY[803836]::bigint[]),
 ('raw'::text,879308::bigint,'stoarama'::text,'managed/acct-47/100018_Europe_Czechia_Vrchlabi_Vrchlabi_-_Nmst_T.G._Masaryka/August/28-Friday/100018_Vrchlabi_-_Nmst_T.G._Masaryka_2026_August_W4_Friday_hour_172704.mp4'::text,ARRAY[879308]::bigint[]),
 ('raw'::text,1455172::bigint,'stoarama'::text,'managed/acct-47/100018_Europe_Czechia_Vrchlabi_Vrchlabi_-_Nmst_T.G._Masaryka/September/18-Friday/100018_Vrchlabi_-_Nmst_T.G._Masaryka_2026_September_W3_Friday_hour_110717.mp4'::text,ARRAY[1455172]::bigint[]),
 ('raw'::text,1437485::bigint,'stoarama'::text,'managed/acct-47/100018_Europe_Czechia_Vrchlabi_Vrchlabi_-_Nmst_T.G._Masaryka/September/17-Thursday/100018_Vrchlabi_-_Nmst_T.G._Masaryka_2026_September_W3_Thursday_hour_123817.mp4'::text,ARRAY[1437485]::bigint[]),
 ('raw'::text,1459981::bigint,'stoarama'::text,'managed/acct-47/100018_Europe_Czechia_Vrchlabi_Vrchlabi_-_Nmst_T.G._Masaryka/September/18-Friday/100018_Vrchlabi_-_Nmst_T.G._Masaryka_2026_September_W3_Friday_hour_160017.mp4'::text,ARRAY[1459981]::bigint[]),
 ('raw'::text,1278687::bigint,'stoarama'::text,'managed/acct-47/100018_Europe_Czechia_Vrchlabi_Vrchlabi_-_Nmst_T.G._Masaryka/September/09-Wednesday/100018_Vrchlabi_-_Nmst_T.G._Masaryka_2026_September_W2_Wednesday_hour_094943.mp4'::text,ARRAY[1278687]::bigint[]),
 ('collated'::text,152088::bigint,'stoarama'::text,'joined/nightly-collation-v2/objects/f2feed40914544c61651d6defbf2e0e2b1766293ce95dacc35704685632efeb3.mp4'::text,ARRAY[2035842,2035863,2035886,2035911,2035935,2035958,2035984,2036009,2036033,2036056,2036079,2036101,2036122,2036145,2036168,2036195,2036221,2036247,2036270,2036293,2036318,2036342,2036367,2036391,2036418,2036442,2036467,2036491,2036514,2036541,2036556]::bigint[]),
 ('collated'::text,139042::bigint,'stoarama'::text,'joined/nightly-collation-v2/objects/2a2e0f70f5b58ae63ff4c5740bae24135547b83a29bdea90ea230f091ae8ce32.mp4'::text,ARRAY[2010514,2010541,2010567,2010593,2010620,2010645,2010673,2010701,2010728,2010755,2010782,2010811,2010837,2010867,2010896,2010922,2010951,2010979,2011005,2011034,2011063,2011091,2011119,2011147,2011176,2011201,2011229,2011257,2011283,2011311,2011321]::bigint[]),
 ('collated'::text,30747::bigint,'stoarama'::text,'joined/nightly-collation-v2/objects/9302ad32766ed560e78aff0c6a2743e1613caf73cb3e72b4f341543e836d2e99.mp4'::text,ARRAY[1753306,1753353,1753399,1753442,1753485,1753523,1753565,1753605,1753648,1753687,1753730,1753770,1753810,1753850,1753889,1753927,1753968,1754009,1754049,1754091,1754134,1754175,1754213,1754255,1754298,1754339,1754378,1754421,1754461,1754503,1754510]::bigint[]),
 ('collated'::text,152074::bigint,'stoarama'::text,'joined/nightly-collation-v2/objects/697e8ed481be3566055a93ad37873accbaca77d4dadf6e2006a9d75a28e98c48.mp4'::text,ARRAY[2026194,2026218,2026251,2026281,2026312,2026344,2026380,2026411,2026444,2026478,2026511,2026546,2026576,2026609,2026644,2026677,2026707,2026739,2026772,2026804,2026836,2026868,2026902,2026942,2026967,2027002,2027036,2027072,2027100,2027134,2027157]::bigint[]),
 ('collated'::text,30757::bigint,'stoarama'::text,'joined/nightly-collation-v2/objects/e2f837f81b439ba46017d2c7a865b1751b3f7f7801df87ddfb268182cb39ed75.mp4'::text,ARRAY[1762912,1762955,1762997,1763034,1763078,1763120,1763163,1763205,1763245,1763289,1763330,1763370,1763410,1763451,1763492,1763531,1763573,1763613,1763655,1763694,1763737,1763778,1763820,1763859,1763900,1763941,1763984,1764022,1764063,1764104,1764117]::bigint[]),
 ('collated'::text,109398::bigint,'stoarama'::text,'joined/nightly-collation-v2/objects/996dfac47ffbcd7375c45c628db5d9342e6e61b6389b90324d1d7d7cf80f4945.mp4'::text,ARRAY[1831012,1831060,1831114,1831170,1831224,1831274,1831329,1831381,1831429,1831485,1831540,1831593,1831644,1831693,1831749,1831804,1831854,1831909,1831960,1832012,1832062,1832116,1832174,1832223,1832276,1832328,1832378,1832431,1832486,1832540,1832546]::bigint[]),
 ('collated'::text,37946::bigint,'stoarama'::text,'joined/nightly-collation-v2/objects/bbc4e8fec387fc7e6d4bfaea8a6b4ce9300df6f1162b9817626225db82ddd77f.mp4'::text,ARRAY[1804873,1804913,1804954,1804995,1805037,1805079,1805117,1805156,1805201,1805241,1805278,1805320,1805365,1805407,1805443,1805486,1805531,1805574,1805612,1805653,1805700,1805740,1805777,1805820,1805863,1805904,1805945,1805985,1806030,1806071,1806078]::bigint[]),
 ('collated'::text,8870::bigint,'stoarama'::text,'joined/nightly-collation-v2/objects/e718a121413f359c455fd63f151ef4a415fd65678f2b17d4c7b9a865e829c25f.mp4'::text,ARRAY[1624601,1624653,1624709,1624764,1624819,1624869,1624923,1624981,1625037,1625085,1625140,1625194,1625250,1625299,1625353,1625409,1625467,1625516,1625569,1625627,1625680,1625733,1625787,1625842,1625898,1625956,1626006,1626060,1626118,1626174]::bigint[]),
 ('raw'::text,1249772::bigint,'stoarama'::text,'managed/acct-47/100018_Europe_Czechia_Vrchlabi_Vrchlabi_-_Nmst_T.G._Masaryka/September/08-Tuesday/100018_Vrchlabi_-_Nmst_T.G._Masaryka_2026_September_W2_Tuesday_hour_131229.mp4'::text,ARRAY[1249772]::bigint[]),
 ('raw'::text,1249794::bigint,'stoarama'::text,'managed/acct-47/100018_Europe_Czechia_Vrchlabi_Vrchlabi_-_Nmst_T.G._Masaryka/September/08-Tuesday/100018_Vrchlabi_-_Nmst_T.G._Masaryka_2026_September_W2_Tuesday_hour_131329.mp4'::text,ARRAY[1249794]::bigint[]),
 ('raw'::text,1011740::bigint,'stoarama'::text,'managed/acct-47/100018_Europe_Czechia_Vrchlabi_Vrchlabi_-_Nmst_T.G._Masaryka/September/01-Tuesday/100018_Vrchlabi_-_Nmst_T.G._Masaryka_2026_September_W1_Tuesday_hour_160957.mp4'::text,ARRAY[1011740]::bigint[]),
 ('raw'::text,806872::bigint,'stoarama'::text,'managed/acct-47/100018_Europe_Czechia_Vrchlabi_Vrchlabi_-_Nmst_T.G._Masaryka/August/26-Wednesday/100018_Vrchlabi_-_Nmst_T.G._Masaryka_2026_August_W4_Wednesday_hour_132711.mp4'::text,ARRAY[806872]::bigint[]),
 ('raw'::text,1043168::bigint,'stoarama'::text,'managed/acct-47/100018_Europe_Czechia_Vrchlabi_Vrchlabi_-_Nmst_T.G._Masaryka/September/02-Wednesday/100018_Vrchlabi_-_Nmst_T.G._Masaryka_2026_September_W1_Wednesday_hour_150724.mp4'::text,ARRAY[1043168]::bigint[]),
 ('raw'::text,1039159::bigint,'stoarama'::text,'managed/acct-47/100018_Europe_Czechia_Vrchlabi_Vrchlabi_-_Nmst_T.G._Masaryka/September/02-Wednesday/100018_Vrchlabi_-_Nmst_T.G._Masaryka_2026_September_W1_Wednesday_hour_121924.mp4'::text,ARRAY[1039159]::bigint[]),
 ('raw'::text,843331::bigint,'stoarama'::text,'managed/acct-47/100018_Europe_Czechia_Vrchlabi_Vrchlabi_-_Nmst_T.G._Masaryka/August/27-Thursday/100018_Vrchlabi_-_Nmst_T.G._Masaryka_2026_August_W4_Thursday_hour_152250.mp4'::text,ARRAY[843331]::bigint[]),
 ('raw'::text,834986::bigint,'stoarama'::text,'managed/acct-47/100018_Europe_Czechia_Vrchlabi_Vrchlabi_-_Nmst_T.G._Masaryka/August/27-Thursday/100018_Vrchlabi_-_Nmst_T.G._Masaryka_2026_August_W4_Thursday_hour_094950.mp4'::text,ARRAY[834986]::bigint[])
 ) incident(kind,source_id,bucket,object_key,raw_source_ids);
-- Preserve compatibility view OIDs and all existing dependent objects.
CREATE OR REPLACE VIEW nas_cleanup_incident_files AS SELECT kind,source_id FROM nas_cleanup_incident_roster;
CREATE OR REPLACE VIEW nas_cleanup_incident_sources AS SELECT DISTINCT unnest(raw_source_ids) clip_id FROM nas_cleanup_incident_roster;
CREATE OR REPLACE FUNCTION guard_nas_cleanup_output() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
 IF (TG_OP='DELETE' OR ROW(NEW.nas_relative_path,NEW.object_key,NEW.sha256,NEW.size_bytes,NEW.start_at,NEW.end_at,NEW.source_clip_ids,NEW.collation_hour_id) IS DISTINCT FROM ROW(OLD.nas_relative_path,OLD.object_key,OLD.sha256,OLD.size_bytes,OLD.start_at,OLD.end_at,OLD.source_clip_ids,OLD.collation_hour_id))
 AND (EXISTS(SELECT 1 FROM nas_cleanup_r2_reserved_files f WHERE f.kind='collated' AND f.source_id=OLD.id)
 OR EXISTS(SELECT 1 FROM nas_cleanup_incident_files f WHERE f.kind='collated' AND f.source_id=OLD.id)
 OR EXISTS(SELECT 1 FROM nas_cleanup_incident_sources s WHERE s.clip_id=ANY(OLD.source_clip_ids))) THEN
 RAISE EXCEPTION 'retention protected: NAS cleanup collated identity hold'; END IF;
 IF TG_OP='DELETE' THEN RETURN OLD;END IF;RETURN NEW;
END $$;
CREATE OR REPLACE FUNCTION guard_r2_retirement_reservation() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF EXISTS(SELECT 1 FROM nas_cleanup_incident_roster i WHERE i.bucket=NEW.bucket AND i.object_key=NEW.object_key)
 OR EXISTS(SELECT 1 FROM nas_cleanup_incident_files i WHERE i.kind=NEW.kind AND i.source_id=NEW.source_id)
 OR (NEW.kind='raw' AND EXISTS(SELECT 1 FROM nas_cleanup_incident_sources i WHERE i.clip_id=NEW.source_id))
 OR (NEW.kind='collated' AND EXISTS(SELECT 1 FROM recording_collation_outputs o JOIN nas_cleanup_incident_sources i ON i.clip_id=ANY(o.source_clip_ids) WHERE o.id=NEW.source_id)) THEN
  RAISE EXCEPTION 'retention protected: permanent incident source or key';
 END IF;
 IF NEW.retirement_class='owner_accepted_outside_window' THEN
  IF EXISTS(SELECT 1 FROM nas_cleanup_owner_reserved_files f WHERE f.kind=NEW.kind AND f.source_id=NEW.source_id)
  OR (NEW.kind='raw' AND nas_cleanup_owner_clip_held(NEW.source_id)) THEN
   RAISE EXCEPTION 'retention protected: owner class recovery hold';
  END IF;
 ELSE
  IF EXISTS(SELECT 1 FROM nas_cleanup_r2_reserved_files f WHERE f.kind=NEW.kind AND f.source_id=NEW.source_id)
  OR (NEW.kind='raw' AND nas_cleanup_clip_held(NEW.source_id)) THEN
   RAISE EXCEPTION 'retention protected: NAS cleanup already reserves R2 recovery';
  END IF;
 END IF;
 RETURN NEW;
END $$;
