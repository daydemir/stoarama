-- A separate sending policy keeps the old fence and all cleanup evidence readable.
-- Installing these objects does not activate the new window.
CREATE TABLE nas_media_keep20_epoch (
 connection_id bigint PRIMARY KEY REFERENCES nas_delivery_retirement_fences(connection_id) CHECK (connection_id=13),
 activated_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE OR REPLACE FUNCTION nas_delivery_work_epoch(connection bigint) RETURNS boolean LANGUAGE sql STABLE AS $$
 SELECT EXISTS(SELECT 1 FROM nas_media_keep20_epoch WHERE connection_id=connection)
$$;

CREATE FUNCTION nas_media_keep20_manifest() RETURNS text LANGUAGE sql IMMUTABLE AS $$
 SELECT $policy$[
 {"end_at": "2026-10-05T15:00:00+00:00", "keep_everything": false, "start_at": "2026-09-15T15:00:00+00:00", "stream_id": 3, "timezone": "Asia/Tokyo"},
 {"end_at": "2026-09-12T15:00:00+00:00", "keep_everything": true, "start_at": "2026-08-23T15:00:00+00:00", "stream_id": 78, "timezone": "Asia/Seoul"},
 {"end_at": "2026-11-04T08:00:00+00:00", "keep_everything": true, "start_at": "2026-10-15T07:00:00+00:00", "stream_id": 94, "timezone": "America/Los_Angeles"},
 {"end_at": "2026-10-24T15:00:00+00:00", "keep_everything": false, "start_at": "2026-10-04T15:00:00+00:00", "stream_id": 158, "timezone": "Asia/Seoul"},
 {"end_at": "2026-08-16T15:00:00+00:00", "keep_everything": false, "start_at": "2026-07-27T15:00:00+00:00", "stream_id": 175, "timezone": "Asia/Seoul"},
 {"end_at": "2026-09-12T07:00:00+00:00", "keep_everything": false, "start_at": "2026-08-23T07:00:00+00:00", "stream_id": 178, "timezone": "America/Los_Angeles"},
 {"end_at": "2026-09-12T07:00:00+00:00", "keep_everything": false, "start_at": "2026-08-23T07:00:00+00:00", "stream_id": 179, "timezone": "America/Los_Angeles"},
 {"end_at": "2026-07-07T07:00:00+00:00", "keep_everything": false, "start_at": "2026-06-17T07:00:00+00:00", "stream_id": 182, "timezone": "America/Los_Angeles"},
 {"end_at": "2026-07-05T15:00:00+00:00", "keep_everything": false, "start_at": "2026-06-15T15:00:00+00:00", "stream_id": 186, "timezone": "Asia/Seoul"},
 {"end_at": "2026-10-11T15:00:00+00:00", "keep_everything": false, "start_at": "2026-09-21T15:00:00+00:00", "stream_id": 195, "timezone": "Asia/Seoul"},
 {"end_at": "2026-09-12T07:00:00+00:00", "keep_everything": false, "start_at": "2026-08-23T07:00:00+00:00", "stream_id": 293, "timezone": "America/Los_Angeles"},
 {"end_at": "2026-09-12T07:00:00+00:00", "keep_everything": false, "start_at": "2026-08-23T07:00:00+00:00", "stream_id": 295, "timezone": "America/Los_Angeles"},
 {"end_at": "2026-09-12T15:00:00+00:00", "keep_everything": true, "start_at": "2026-08-23T15:00:00+00:00", "stream_id": 415, "timezone": "Asia/Seoul"},
 {"end_at": "2026-08-02T07:00:00+00:00", "keep_everything": false, "start_at": "2026-07-13T07:00:00+00:00", "stream_id": 469, "timezone": "America/Los_Angeles"},
 {"end_at": "2026-08-02T07:00:00+00:00", "keep_everything": false, "start_at": "2026-07-13T07:00:00+00:00", "stream_id": 487, "timezone": "America/Los_Angeles"},
 {"end_at": "2026-08-02T07:00:00+00:00", "keep_everything": false, "start_at": "2026-07-13T07:00:00+00:00", "stream_id": 2666, "timezone": "America/Los_Angeles"},
 {"end_at": "2026-09-12T07:00:00+00:00", "keep_everything": false, "start_at": "2026-08-23T07:00:00+00:00", "stream_id": 2667, "timezone": "America/Los_Angeles"},
 {"end_at": "2026-08-02T07:00:00+00:00", "keep_everything": false, "start_at": "2026-07-13T07:00:00+00:00", "stream_id": 2674, "timezone": "America/Los_Angeles"},
 {"end_at": "2026-08-02T07:00:00+00:00", "keep_everything": false, "start_at": "2026-07-13T07:00:00+00:00", "stream_id": 2675, "timezone": "America/Los_Angeles"},
 {"end_at": "2026-08-02T07:00:00+00:00", "keep_everything": false, "start_at": "2026-07-13T07:00:00+00:00", "stream_id": 2676, "timezone": "America/Los_Angeles"},
 {"end_at": "2026-09-12T07:00:00+00:00", "keep_everything": false, "start_at": "2026-08-23T07:00:00+00:00", "stream_id": 2677, "timezone": "America/Los_Angeles"},
 {"end_at": "2026-08-02T07:00:00+00:00", "keep_everything": false, "start_at": "2026-07-13T07:00:00+00:00", "stream_id": 2678, "timezone": "America/Los_Angeles"},
 {"end_at": "2026-08-02T07:00:00+00:00", "keep_everything": false, "start_at": "2026-07-13T07:00:00+00:00", "stream_id": 2680, "timezone": "America/Los_Angeles"},
 {"end_at": "2026-08-02T07:00:00+00:00", "keep_everything": false, "start_at": "2026-07-13T07:00:00+00:00", "stream_id": 2681, "timezone": "America/Los_Angeles"},
 {"end_at": "2026-08-02T07:00:00+00:00", "keep_everything": false, "start_at": "2026-07-13T07:00:00+00:00", "stream_id": 2693, "timezone": "America/Los_Angeles"},
 {"end_at": "2026-08-02T07:00:00+00:00", "keep_everything": false, "start_at": "2026-07-13T07:00:00+00:00", "stream_id": 2708, "timezone": "America/Los_Angeles"},
 {"end_at": "2026-09-13T07:00:00+00:00", "keep_everything": false, "start_at": "2026-08-24T07:00:00+00:00", "stream_id": 2713, "timezone": "America/Los_Angeles"},
 {"end_at": "2026-08-02T07:00:00+00:00", "keep_everything": false, "start_at": "2026-07-13T07:00:00+00:00", "stream_id": 2717, "timezone": "America/Los_Angeles"},
 {"end_at": "2026-08-02T07:00:00+00:00", "keep_everything": false, "start_at": "2026-07-13T07:00:00+00:00", "stream_id": 2718, "timezone": "America/Los_Angeles"},
 {"end_at": "2026-08-02T07:00:00+00:00", "keep_everything": false, "start_at": "2026-07-13T07:00:00+00:00", "stream_id": 2720, "timezone": "America/Los_Angeles"},
 {"end_at": "2026-08-02T07:00:00+00:00", "keep_everything": false, "start_at": "2026-07-13T07:00:00+00:00", "stream_id": 2726, "timezone": "America/Los_Angeles"},
 {"end_at": "2026-07-25T07:00:00+00:00", "keep_everything": false, "start_at": "2026-07-05T07:00:00+00:00", "stream_id": 2740, "timezone": "America/Los_Angeles"},
 {"end_at": "2026-08-02T07:00:00+00:00", "keep_everything": false, "start_at": "2026-07-13T07:00:00+00:00", "stream_id": 2747, "timezone": "America/Los_Angeles"},
 {"end_at": "2026-08-02T07:00:00+00:00", "keep_everything": false, "start_at": "2026-07-13T07:00:00+00:00", "stream_id": 2757, "timezone": "America/Los_Angeles"},
 {"end_at": "2026-08-02T07:00:00+00:00", "keep_everything": false, "start_at": "2026-07-13T07:00:00+00:00", "stream_id": 2963, "timezone": "America/Los_Angeles"},
 {"end_at": "2026-08-02T07:00:00+00:00", "keep_everything": false, "start_at": "2026-07-13T07:00:00+00:00", "stream_id": 2965, "timezone": "America/Los_Angeles"},
 {"end_at": "2026-08-02T07:00:00+00:00", "keep_everything": false, "start_at": "2026-07-13T07:00:00+00:00", "stream_id": 2973, "timezone": "America/Los_Angeles"},
 {"end_at": "2026-09-12T07:00:00+00:00", "keep_everything": false, "start_at": "2026-08-23T07:00:00+00:00", "stream_id": 2975, "timezone": "America/Los_Angeles"},
 {"end_at": "2026-09-12T07:00:00+00:00", "keep_everything": false, "start_at": "2026-08-23T07:00:00+00:00", "stream_id": 3006, "timezone": "America/Los_Angeles"},
 {"end_at": "2026-08-02T07:00:00+00:00", "keep_everything": false, "start_at": "2026-07-13T07:00:00+00:00", "stream_id": 3007, "timezone": "America/Los_Angeles"},
 {"end_at": "2026-09-12T07:00:00+00:00", "keep_everything": false, "start_at": "2026-08-23T07:00:00+00:00", "stream_id": 3018, "timezone": "America/Los_Angeles"},
 {"end_at": "2026-09-12T07:00:00+00:00", "keep_everything": false, "start_at": "2026-08-23T07:00:00+00:00", "stream_id": 3021, "timezone": "America/Los_Angeles"},
 {"end_at": "2026-09-12T07:00:00+00:00", "keep_everything": false, "start_at": "2026-08-23T07:00:00+00:00", "stream_id": 3022, "timezone": "America/Los_Angeles"},
 {"end_at": "2026-09-12T07:00:00+00:00", "keep_everything": false, "start_at": "2026-08-23T07:00:00+00:00", "stream_id": 3025, "timezone": "America/Los_Angeles"},
 {"end_at": "2026-08-02T07:00:00+00:00", "keep_everything": false, "start_at": "2026-07-13T07:00:00+00:00", "stream_id": 3039, "timezone": "America/Los_Angeles"},
 {"end_at": "2026-08-02T07:00:00+00:00", "keep_everything": false, "start_at": "2026-07-13T07:00:00+00:00", "stream_id": 3049, "timezone": "America/Los_Angeles"},
 {"end_at": "2026-09-12T03:00:00+00:00", "keep_everything": false, "start_at": "2026-08-23T03:00:00+00:00", "stream_id": 12672, "timezone": "America/Bahia"},
 {"end_at": "2026-09-02T03:00:00+00:00", "keep_everything": false, "start_at": "2026-08-13T03:00:00+00:00", "stream_id": 12725, "timezone": "America/Punta_Arenas"},
 {"end_at": "2026-09-28T21:00:00+00:00", "keep_everything": true, "start_at": "2026-09-08T21:00:00+00:00", "stream_id": 14303, "timezone": "Asia/Amman"},
 {"end_at": "2026-09-06T05:00:00+00:00", "keep_everything": true, "start_at": "2026-08-17T05:00:00+00:00", "stream_id": 14478, "timezone": "America/Lima"},
 {"end_at": "2026-10-13T22:00:00+00:00", "keep_everything": true, "start_at": "2026-09-23T22:00:00+00:00", "stream_id": 14554, "timezone": "Africa/Johannesburg"},
 {"end_at": "2026-10-13T21:00:00+00:00", "keep_everything": false, "start_at": "2026-09-23T21:00:00+00:00", "stream_id": 14782, "timezone": "Africa/Dar_es_Salaam"},
 {"end_at": "2026-09-12T07:00:00+00:00", "keep_everything": false, "start_at": "2026-08-23T07:00:00+00:00", "stream_id": 17186, "timezone": "America/Los_Angeles"},
 {"end_at": "2026-09-28T22:00:00+00:00", "keep_everything": true, "start_at": "2026-09-08T22:00:00+00:00", "stream_id": 17200, "timezone": "Europe/Berlin"},
 {"end_at": "2026-08-25T22:00:00+00:00", "keep_everything": false, "start_at": "2026-08-05T22:00:00+00:00", "stream_id": 17208, "timezone": "Europe/Prague"},
 {"end_at": "2026-08-08T22:00:00+00:00", "keep_everything": false, "start_at": "2026-07-19T22:00:00+00:00", "stream_id": 17216, "timezone": "Europe/Warsaw"},
 {"end_at": "2026-08-07T22:00:00+00:00", "keep_everything": false, "start_at": "2026-07-18T22:00:00+00:00", "stream_id": 17219, "timezone": "Europe/Warsaw"},
 {"end_at": "2026-08-12T22:00:00+00:00", "keep_everything": false, "start_at": "2026-07-23T22:00:00+00:00", "stream_id": 17233, "timezone": "Europe/Rome"},
 {"end_at": "2026-08-19T17:00:00+00:00", "keep_everything": false, "start_at": "2026-07-30T17:00:00+00:00", "stream_id": 17234, "timezone": "Asia/Bangkok"},
 {"end_at": "2026-08-11T16:00:00+00:00", "keep_everything": true, "start_at": "2026-07-22T16:00:00+00:00", "stream_id": 17235, "timezone": "Asia/Manila"},
 {"end_at": "2026-08-10T15:00:00+00:00", "keep_everything": false, "start_at": "2026-07-21T15:00:00+00:00", "stream_id": 17237, "timezone": "Asia/Seoul"},
 {"end_at": "2026-08-11T07:00:00+00:00", "keep_everything": false, "start_at": "2026-07-22T07:00:00+00:00", "stream_id": 17238, "timezone": "America/Los_Angeles"},
 {"end_at": "2026-08-10T15:00:00+00:00", "keep_everything": true, "start_at": "2026-07-21T15:00:00+00:00", "stream_id": 17239, "timezone": "Asia/Tokyo"},
 {"end_at": "2026-08-11T15:00:00+00:00", "keep_everything": false, "start_at": "2026-07-22T15:00:00+00:00", "stream_id": 17240, "timezone": "Asia/Tokyo"},
 {"end_at": "2026-08-19T07:00:00+00:00", "keep_everything": false, "start_at": "2026-07-30T07:00:00+00:00", "stream_id": 17241, "timezone": "America/Los_Angeles"},
 {"end_at": "2026-09-12T07:00:00+00:00", "keep_everything": false, "start_at": "2026-08-23T07:00:00+00:00", "stream_id": 17242, "timezone": "America/Los_Angeles"},
 {"end_at": "2026-08-11T04:00:00+00:00", "keep_everything": false, "start_at": "2026-07-22T04:00:00+00:00", "stream_id": 17243, "timezone": "America/New_York"},
 {"end_at": "2026-08-11T17:00:00+00:00", "keep_everything": false, "start_at": "2026-07-22T17:00:00+00:00", "stream_id": 17247, "timezone": "Asia/Bangkok"},
 {"end_at": "2026-09-12T07:00:00+00:00", "keep_everything": false, "start_at": "2026-08-23T07:00:00+00:00", "stream_id": 17248, "timezone": "America/Los_Angeles"},
 {"end_at": "2026-09-12T07:00:00+00:00", "keep_everything": false, "start_at": "2026-08-23T07:00:00+00:00", "stream_id": 17249, "timezone": "America/Los_Angeles"},
 {"end_at": "2026-10-20T15:00:00+00:00", "keep_everything": false, "start_at": "2026-09-30T15:00:00+00:00", "stream_id": 17251, "timezone": "Asia/Seoul"},
 {"end_at": "2026-10-20T15:00:00+00:00", "keep_everything": false, "start_at": "2026-09-30T15:00:00+00:00", "stream_id": 17254, "timezone": "Asia/Seoul"},
 {"end_at": "2026-10-20T15:00:00+00:00", "keep_everything": false, "start_at": "2026-09-30T15:00:00+00:00", "stream_id": 17256, "timezone": "Asia/Seoul"},
 {"end_at": "2026-10-20T15:00:00+00:00", "keep_everything": false, "start_at": "2026-09-30T15:00:00+00:00", "stream_id": 17261, "timezone": "Asia/Seoul"},
 {"end_at": "2026-10-20T15:00:00+00:00", "keep_everything": false, "start_at": "2026-09-30T15:00:00+00:00", "stream_id": 17268, "timezone": "Asia/Seoul"},
 {"end_at": "2026-08-23T22:00:00+00:00", "keep_everything": true, "start_at": "2026-08-03T22:00:00+00:00", "stream_id": 17309, "timezone": "Europe/Rome"},
 {"end_at": "2026-08-23T22:00:00+00:00", "keep_everything": true, "start_at": "2026-08-03T22:00:00+00:00", "stream_id": 17310, "timezone": "Europe/Stockholm"},
 {"end_at": "2026-08-23T22:00:00+00:00", "keep_everything": true, "start_at": "2026-08-03T22:00:00+00:00", "stream_id": 17311, "timezone": "Europe/Prague"},
 {"end_at": "2026-08-25T22:00:00+00:00", "keep_everything": false, "start_at": "2026-08-05T22:00:00+00:00", "stream_id": 17312, "timezone": "Europe/Sarajevo"},
 {"end_at": "2026-08-23T22:00:00+00:00", "keep_everything": true, "start_at": "2026-08-03T22:00:00+00:00", "stream_id": 17313, "timezone": "Europe/Amsterdam"},
 {"end_at": "2026-08-20T22:00:00+00:00", "keep_everything": false, "start_at": "2026-07-31T22:00:00+00:00", "stream_id": 17314, "timezone": "Europe/Belgrade"},
 {"end_at": "2026-08-25T22:00:00+00:00", "keep_everything": false, "start_at": "2026-08-05T22:00:00+00:00", "stream_id": 17315, "timezone": "Europe/Madrid"},
 {"end_at": "2026-08-31T22:00:00+00:00", "keep_everything": false, "start_at": "2026-08-11T22:00:00+00:00", "stream_id": 17316, "timezone": "Europe/Madrid"},
 {"end_at": "2026-09-11T22:00:00+00:00", "keep_everything": true, "start_at": "2026-08-22T22:00:00+00:00", "stream_id": 17317, "timezone": "Europe/Prague"},
 {"end_at": "2026-10-13T22:00:00+00:00", "keep_everything": true, "start_at": "2026-09-23T22:00:00+00:00", "stream_id": 17318, "timezone": "Europe/Prague"},
 {"end_at": "2026-09-11T22:00:00+00:00", "keep_everything": false, "start_at": "2026-08-22T22:00:00+00:00", "stream_id": 17319, "timezone": "Europe/Budapest"},
 {"end_at": "2026-10-13T22:00:00+00:00", "keep_everything": false, "start_at": "2026-09-23T22:00:00+00:00", "stream_id": 17320, "timezone": "Europe/Warsaw"},
 {"end_at": "2026-08-25T22:00:00+00:00", "keep_everything": false, "start_at": "2026-08-05T22:00:00+00:00", "stream_id": 17321, "timezone": "Europe/Warsaw"},
 {"end_at": "2026-10-12T22:00:00+00:00", "keep_everything": false, "start_at": "2026-09-22T22:00:00+00:00", "stream_id": 17322, "timezone": "Europe/Warsaw"},
 {"end_at": "2026-08-23T22:00:00+00:00", "keep_everything": false, "start_at": "2026-08-03T22:00:00+00:00", "stream_id": 17323, "timezone": "Europe/Warsaw"},
 {"end_at": "2026-10-13T22:00:00+00:00", "keep_everything": false, "start_at": "2026-09-23T22:00:00+00:00", "stream_id": 17324, "timezone": "Europe/Warsaw"},
 {"end_at": "2026-08-16T22:00:00+00:00", "keep_everything": false, "start_at": "2026-07-27T22:00:00+00:00", "stream_id": 17325, "timezone": "Europe/Warsaw"},
 {"end_at": "2026-08-29T22:00:00+00:00", "keep_everything": false, "start_at": "2026-08-09T22:00:00+00:00", "stream_id": 17326, "timezone": "Europe/Warsaw"},
 {"end_at": "2026-09-27T22:00:00+00:00", "keep_everything": true, "start_at": "2026-09-07T22:00:00+00:00", "stream_id": 17327, "timezone": "Europe/Warsaw"},
 {"end_at": "2026-10-13T22:00:00+00:00", "keep_everything": false, "start_at": "2026-09-23T22:00:00+00:00", "stream_id": 17328, "timezone": "Europe/Warsaw"},
 {"end_at": "2026-10-12T22:00:00+00:00", "keep_everything": false, "start_at": "2026-09-22T22:00:00+00:00", "stream_id": 17329, "timezone": "Europe/Warsaw"},
 {"end_at": "2026-10-12T22:00:00+00:00", "keep_everything": true, "start_at": "2026-09-22T22:00:00+00:00", "stream_id": 17330, "timezone": "Europe/Warsaw"},
 {"end_at": "2026-08-23T22:00:00+00:00", "keep_everything": true, "start_at": "2026-08-03T22:00:00+00:00", "stream_id": 17331, "timezone": "Europe/Rome"},
 {"end_at": "2026-08-23T22:00:00+00:00", "keep_everything": true, "start_at": "2026-08-03T22:00:00+00:00", "stream_id": 17332, "timezone": "Europe/Bratislava"},
 {"end_at": "2026-08-23T22:00:00+00:00", "keep_everything": true, "start_at": "2026-08-03T22:00:00+00:00", "stream_id": 17333, "timezone": "Europe/Prague"},
 {"end_at": "2026-08-23T22:00:00+00:00", "keep_everything": true, "start_at": "2026-08-03T22:00:00+00:00", "stream_id": 17334, "timezone": "Europe/Prague"},
 {"end_at": "2026-08-23T22:00:00+00:00", "keep_everything": true, "start_at": "2026-08-03T22:00:00+00:00", "stream_id": 17335, "timezone": "Europe/Bratislava"},
 {"end_at": "2026-08-23T22:00:00+00:00", "keep_everything": false, "start_at": "2026-08-03T22:00:00+00:00", "stream_id": 17336, "timezone": "Europe/Prague"},
 {"end_at": "2026-11-05T23:00:00+00:00", "keep_everything": false, "start_at": "2026-10-16T22:00:00+00:00", "stream_id": 17337, "timezone": "Europe/Warsaw"},
 {"end_at": "2026-10-12T22:00:00+00:00", "keep_everything": false, "start_at": "2026-09-22T22:00:00+00:00", "stream_id": 17338, "timezone": "Europe/Warsaw"},
 {"end_at": "2026-08-24T22:00:00+00:00", "keep_everything": false, "start_at": "2026-08-04T22:00:00+00:00", "stream_id": 17339, "timezone": "Europe/Warsaw"},
 {"end_at": "2026-08-23T22:00:00+00:00", "keep_everything": false, "start_at": "2026-08-03T22:00:00+00:00", "stream_id": 17340, "timezone": "Europe/Warsaw"},
 {"end_at": "2026-08-23T22:00:00+00:00", "keep_everything": true, "start_at": "2026-08-03T22:00:00+00:00", "stream_id": 17341, "timezone": "Europe/Berlin"},
 {"end_at": "2026-08-28T22:00:00+00:00", "keep_everything": false, "start_at": "2026-08-08T22:00:00+00:00", "stream_id": 17342, "timezone": "Europe/Warsaw"}
]$policy$::text
$$;

CREATE TABLE nas_media_keep20_windows (
 stream_id bigint PRIMARY KEY,
 timezone text NOT NULL,
 start_at timestamptz NOT NULL,
 end_at timestamptz NOT NULL CHECK(end_at>start_at),
 keep_everything boolean NOT NULL
);
INSERT INTO nas_media_keep20_windows
 SELECT * FROM jsonb_to_recordset(nas_media_keep20_manifest()::jsonb)
 AS w(stream_id bigint,timezone text,start_at timestamptz,end_at timestamptz,keep_everything boolean);

CREATE TRIGGER nas_media_keep20_windows_immutable BEFORE INSERT OR UPDATE OR DELETE ON nas_media_keep20_windows
 FOR EACH ROW EXECUTE FUNCTION guard_nas_delivery_immutable();
CREATE TRIGGER nas_media_keep20_windows_no_truncate BEFORE TRUNCATE ON nas_media_keep20_windows
 FOR EACH STATEMENT EXECUTE FUNCTION guard_nas_delivery_immutable();
CREATE TRIGGER nas_media_keep20_epoch_immutable BEFORE UPDATE OR DELETE ON nas_media_keep20_epoch
 FOR EACH ROW EXECUTE FUNCTION guard_nas_delivery_immutable();
CREATE TRIGGER nas_media_keep20_epoch_no_truncate BEFORE TRUNCATE ON nas_media_keep20_epoch
 FOR EACH STATEMENT EXECUTE FUNCTION guard_nas_delivery_immutable();

CREATE OR REPLACE VIEW nas_delivery_work_windows AS
 SELECT connection_id,stream_id,timezone,start_at,end_at FROM nas_delivery_retirement_windows
 WHERE NOT EXISTS(SELECT 1 FROM nas_media_keep20_epoch)
 UNION ALL
 SELECT 13,stream_id,timezone,start_at,end_at FROM nas_media_keep20_windows
 WHERE NOT keep_everything AND EXISTS(SELECT 1 FROM nas_media_keep20_epoch);

CREATE FUNCTION nas_media_keep20_span(stream bigint,tz text,a timestamptz,b timestamptz) RETURNS integer LANGUAGE sql STABLE AS $$
 SELECT coalesce((SELECT CASE WHEN a IS NULL OR b IS NULL OR b<=a OR a<'2020-01-01' OR b>='2028-01-01'
  OR timezone<>tz OR tz IS NULL THEN 0
 WHEN a<end_at AND b>start_at THEN 1 ELSE -1 END
 FROM nas_media_keep20_windows WHERE stream_id=stream AND NOT keep_everything),0)
$$;

CREATE FUNCTION nas_media_keep20_copy_item(i jsonb) RETURNS boolean LANGUAGE sql STABLE AS $$
 SELECT EXISTS(SELECT 1 FROM nas_media_keep20_windows w WHERE NOT w.keep_everything
  AND w.stream_id=(i->>'stream_id')::bigint AND (i->>'end_at')::timestamptz>(i->>'start_at')::timestamptz
  AND (i->>'start_at')::timestamptz<w.end_at AND (i->>'end_at')::timestamptz>w.start_at)
$$;

CREATE FUNCTION nas_media_keep20_scope(stream bigint,a timestamptz,b timestamptz) RETURNS boolean LANGUAGE sql STABLE AS $$
 SELECT EXISTS(SELECT 1 FROM nas_media_keep20_windows w WHERE NOT w.keep_everything
  AND w.stream_id=stream AND a>=w.start_at AND b<=w.end_at AND b>a)
$$;

CREATE FUNCTION nas_media_keep20_stitch_plan(p jsonb) RETURNS boolean LANGUAGE sql STABLE AS $$
 SELECT coalesce(jsonb_array_length(p->'clips')>0 AND NOT EXISTS(
  SELECT 1 FROM jsonb_array_elements(p->'clips') c
  WHERE NOT nas_media_keep20_scope((p->>'stream_id')::bigint,
   (c->>'start_at')::timestamptz,(c->>'end_at')::timestamptz)),false)
$$;

CREATE FUNCTION nas_media_keep20_activate_guard() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF current_setting('transaction_isolation')<>'read committed' THEN
  RAISE EXCEPTION 'NAS media activation requires READ COMMITTED' USING ERRCODE='55000';
 END IF;
 PERFORM pg_advisory_xact_lock(7343226011110); -- stitch lease fence
 PERFORM pg_advisory_xact_lock(7343226011192); -- copy lease fence, before cleanup
 PERFORM pg_advisory_xact_lock(7343226011101); -- cleanup public fence
 PERFORM pg_advisory_xact_lock(7343226011185); -- ordinary delivery
 IF NOT pg_try_advisory_xact_lock(7343226011187) THEN
  RAISE EXCEPTION 'R2 stitch staging admission is busy' USING ERRCODE='55P03';
 END IF;
 IF NOT EXISTS(SELECT 1 FROM nas_delivery_retirement_fences WHERE connection_id=13
     AND policy_sha256='060aa7070c7909d78013415f83b76bd0c027a885caf91c67c8b709ac1f4b5811')
  OR (SELECT count(*) FROM nas_media_keep20_windows)<>109
  OR EXISTS(SELECT 1 FROM nas_media_keep20_windows w LEFT JOIN nas_delivery_retirement_windows prior
     ON prior.connection_id=13 AND prior.stream_id=w.stream_id WHERE prior.stream_id IS NULL
     OR prior.timezone<>w.timezone OR w.start_at<prior.start_at OR w.end_at>prior.end_at)
  OR encode(sha256(convert_to(nas_media_keep20_manifest(),'UTF8')),'hex')<>'bcc670a34024c2f8e2ba3df2566397f370d03e93ff33a7bb828c4b83eb123393' THEN
  RAISE EXCEPTION 'NAS 20-day pin does not match its frozen inputs';
 END IF;
 IF EXISTS(SELECT 1 FROM nas_cleanup_plans WHERE mode='enabled')
  OR EXISTS(SELECT 1 FROM nas_cleanup_batches WHERE mode='enabled')
  OR EXISTS(SELECT 1 FROM nas_cleanup_items WHERE state='leased')
  OR EXISTS(SELECT 1 FROM nas_cleanup_maintenance WHERE active)
  OR EXISTS(SELECT 1 FROM connections WHERE id=13 AND delivery_nas_handoff_enabled)
  OR EXISTS(SELECT 1 FROM nas_stitch_r2_stage_objects
      WHERE state IN ('starting','uploading','completing') OR grants_drain_at>=clock_timestamp())
  OR EXISTS(SELECT 1 FROM nas_r2_copy_items WHERE connection_id=13
     AND (state IN ('leased','refresh_leased') AND lease_expires_at>clock_timestamp()
       OR state IN ('pending','refresh_pending','leased','refresh_leased') AND NOT nas_media_keep20_copy_item(item)))
  OR EXISTS(SELECT 1 FROM nas_stitch_r2_hours WHERE state='leased' AND lease_expires_at>clock_timestamp())
  OR EXISTS(SELECT 1 FROM nas_stitch_tasks WHERE (state='leased' AND lease_expires_at>clock_timestamp())
       OR (state='verified' AND cleanup_plan_id IS NULL AND registration_abandoned_at IS NULL))
  OR EXISTS(
    SELECT 1 FROM nas_stitch_tasks t JOIN nas_stitch_scopes s ON s.id=t.scope_id
      WHERE t.state IN ('pending','leased') AND NOT s.paused AND s.approved_until>clock_timestamp()
      AND NOT nas_media_keep20_stitch_plan(t.plan)
    UNION ALL
    SELECT 1 FROM nas_stitch_r2_hours t JOIN nas_stitch_scopes s ON s.id=t.scope_id
      WHERE t.state IN ('pending','leased') AND NOT s.paused AND s.approved_until>clock_timestamp()
      AND NOT nas_media_keep20_stitch_plan(t.plan)
  )
  OR EXISTS(SELECT 1 FROM nas_stitch_scopes WHERE NOT paused AND approved_until>clock_timestamp()
     AND NOT nas_media_keep20_scope(stream_id,start_at,end_at)) THEN
  RAISE EXCEPTION 'NAS 20-day switch requires drained work and in-scope pending rows';
 END IF;
 -- Legacy handoff validates under REPEATABLE READ. Touch the exact row it
 -- updates so a snapshot predating the epoch cannot enable it after commit.
 UPDATE connections SET delivery_nas_handoff_enabled=false
  WHERE id=13 AND NOT delivery_nas_handoff_enabled;
 IF NOT FOUND THEN RAISE EXCEPTION 'legacy NAS handoff is active'; END IF;
 NEW.activated_at := clock_timestamp();
 RETURN NEW;
END $$;
CREATE TRIGGER nas_media_keep20_activate BEFORE INSERT ON nas_media_keep20_epoch
 FOR EACH ROW EXECUTE FUNCTION nas_media_keep20_activate_guard();

CREATE FUNCTION nas_media_keep20_guard_handoff() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF NEW.id=13 AND NEW.delivery_nas_handoff_enabled THEN
  IF NOT pg_try_advisory_xact_lock_shared(7343226011185) THEN
   RAISE EXCEPTION 'NAS media work window busy' USING ERRCODE='55P03';
  END IF;
  IF EXISTS(SELECT 1 FROM nas_media_keep20_epoch) THEN
   RAISE EXCEPTION 'legacy NAS handoff is outside the active 20-day protocol';
  END IF;
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER nas_media_keep20_legacy_handoff BEFORE UPDATE OF delivery_nas_handoff_enabled ON connections
 FOR EACH ROW EXECUTE FUNCTION nas_media_keep20_guard_handoff();

CREATE FUNCTION nas_media_keep20_guard_copy() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF NEW.connection_id=13 AND NEW.state IN ('pending','leased','refresh_pending','refresh_leased') THEN
  IF current_setting('transaction_isolation')<>'read committed' THEN
   RAISE EXCEPTION 'NAS copy admission requires READ COMMITTED' USING ERRCODE='55000';
  END IF;
  IF NOT pg_try_advisory_xact_lock_shared(7343226011192) THEN
   RAISE EXCEPTION 'NAS copy work window busy' USING ERRCODE='55P03';
  END IF;
  IF EXISTS(SELECT 1 FROM nas_media_keep20_epoch) AND NOT nas_media_keep20_copy_item(NEW.item) THEN
   RAISE EXCEPTION 'NAS copy outside the active 20-day work window';
  END IF;
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER nas_media_keep20_copy BEFORE INSERT OR UPDATE OF state,item ON nas_r2_copy_items
 FOR EACH ROW EXECUTE FUNCTION nas_media_keep20_guard_copy();

CREATE FUNCTION nas_media_keep20_guard_scope() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF NOT NEW.paused THEN
  IF current_setting('transaction_isolation')<>'read committed' THEN
   RAISE EXCEPTION 'NAS stitch scope admission requires READ COMMITTED' USING ERRCODE='55000';
  END IF;
  IF NOT pg_try_advisory_xact_lock_shared(7343226011110) THEN
   RAISE EXCEPTION 'NAS stitch work window busy' USING ERRCODE='55P03';
  END IF;
  IF EXISTS(SELECT 1 FROM nas_media_keep20_epoch)
    AND NOT nas_media_keep20_scope(NEW.stream_id,NEW.start_at,NEW.end_at) THEN
   RAISE EXCEPTION 'stitch scope outside the active 20-day work window';
  END IF;
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER nas_media_keep20_stitch_scope BEFORE INSERT OR UPDATE ON nas_stitch_scopes
 FOR EACH ROW EXECUTE FUNCTION nas_media_keep20_guard_scope();

CREATE FUNCTION nas_media_keep20_guard_stitch_plan() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF NEW.state IN ('pending','leased') THEN
  IF current_setting('transaction_isolation')<>'read committed' THEN
   RAISE EXCEPTION 'NAS stitch plan admission requires READ COMMITTED' USING ERRCODE='55000';
  END IF;
  IF NOT pg_try_advisory_xact_lock_shared(7343226011110) THEN
   RAISE EXCEPTION 'NAS stitch work window busy' USING ERRCODE='55P03';
  END IF;
  IF EXISTS(SELECT 1 FROM nas_media_keep20_epoch) AND NOT nas_media_keep20_stitch_plan(NEW.plan) THEN
   RAISE EXCEPTION 'stitch plan outside the active 20-day work window';
  END IF;
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER nas_media_keep20_stitch_plan BEFORE INSERT OR UPDATE OF state,plan ON nas_stitch_tasks
 FOR EACH ROW EXECUTE FUNCTION nas_media_keep20_guard_stitch_plan();
CREATE TRIGGER nas_media_keep20_r2_stitch_plan BEFORE INSERT OR UPDATE OF state,plan ON nas_stitch_r2_hours
 FOR EACH ROW EXECUTE FUNCTION nas_media_keep20_guard_stitch_plan();

-- Leave nas_delivery_object_span and its historical retirement evidence
-- intact. Only admission of new ordinary NAS work uses the new epoch.
CREATE OR REPLACE FUNCTION nas_delivery_allowed(connection bigint,kind text,source bigint)
RETURNS boolean LANGUAGE plpgsql STABLE AS $$
BEGIN
 IF NOT EXISTS(SELECT 1 FROM nas_delivery_retirement_fences WHERE connection_id=connection) THEN
  RETURN true;
 END IF;
 IF NOT EXISTS(SELECT 1 FROM nas_media_keep20_epoch WHERE connection_id=connection) THEN
  RETURN nas_delivery_object_span(kind,source)=1;
 END IF;
 IF kind='raw' THEN
  RETURN coalesce((SELECT nas_media_keep20_span(r.stream_id,coalesce(nullif(s.local_timezone,''),r.cron_timezone),
   c.clip_start_at,c.clip_end_at)=1
   FROM recording_clips c JOIN recordings r ON r.id=c.recording_id LEFT JOIN streams s ON s.id=r.stream_id
   WHERE c.id=source AND r.account_id=47),false);
 ELSIF kind='collated' THEN
  RETURN coalesce((SELECT nas_media_keep20_span(r.stream_id,coalesce(nullif(s.local_timezone,''),r.cron_timezone),
    o.start_at,o.end_at)=1 AND cardinality(o.source_clip_ids)>0 AND NOT EXISTS (
     SELECT 1 FROM unnest(o.source_clip_ids) ids(id)
     LEFT JOIN recording_clips c ON c.id=ids.id LEFT JOIN recordings cr ON cr.id=c.recording_id
     LEFT JOIN streams cs ON cs.id=cr.stream_id
     WHERE cr.account_id IS DISTINCT FROM 47 OR nas_media_keep20_span(cr.stream_id,
      coalesce(nullif(cs.local_timezone,''),cr.cron_timezone),c.clip_start_at,c.clip_end_at)=0)
   FROM recording_collation_outputs o JOIN recording_collation_hours h ON h.id=o.collation_hour_id
   JOIN recordings r ON r.id=h.recording_id LEFT JOIN streams s ON s.id=r.stream_id
   WHERE o.id=source AND r.account_id=47),false);
 ELSIF kind='joined' THEN
  RETURN coalesce((SELECT a.artifact_kind='batch_index' OR nas_media_keep20_span(r.stream_id,
   coalesce(nullif(s.local_timezone,''),r.cron_timezone),coalesce(h.scheduled_start_at,d.scheduled_start_at),
   coalesce(h.scheduled_end_at,d.scheduled_end_at))=1
   FROM recording_joined_artifacts a LEFT JOIN recording_joined_hours h ON h.id=a.hour_record_id
   LEFT JOIN recording_joined_stream_days d ON d.id=a.stream_day_id
   LEFT JOIN recordings r ON r.id=coalesce(h.recording_id,d.recording_id) LEFT JOIN streams s ON s.id=r.stream_id
   WHERE a.id=source AND a.connection_id=connection),false);
 END IF;
 RETURN false;
END $$;
