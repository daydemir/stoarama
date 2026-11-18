-- Owner-authorized timing exception, 2026-11-18. Deniz, approximately 15:50Z,
-- verbatim: 'what the flying fuck is this safety defense. we can delete its fine'
-- Deniz, approximately 16:05Z, verbatim:
-- 'lets just go faster now, as long as we're only deleting footage that is all of / 1. not in the keep everythign set / 2. not in the 28 days set AND not on the NAS (ie waiting to be transferred) / then just delete it all now no need to limit 5tb / day'
-- The migrator runs this entire file in one transaction. Preserve the original
-- activation/endpoint and the immutable-fence trigger. This is not PUT-completion
-- evidence; the executor enforces the separately reviewed pre-fence object cutoff.
LOCK TABLE r2_retirement_writer_fences IN ACCESS EXCLUSIVE MODE;
DO $$ BEGIN
 IF EXISTS(SELECT 1 FROM r2_retirement_writer_fences WHERE bucket='stoarama') THEN
  IF NOT pg_try_advisory_xact_lock(7343226011187) THEN
   RAISE EXCEPTION 'R2 wire writes or admissions still in flight';
  END IF;
  IF NOT pg_try_advisory_xact_lock(7343226011173) THEN
   RAISE EXCEPTION 'R2 retirement apply is active';
  END IF;
 END IF;
END $$;
ALTER TABLE r2_retirement_writer_fences DISABLE TRIGGER r2_writer_fence_transition;
UPDATE r2_retirement_writer_fences SET drain_not_before=now() WHERE bucket='stoarama';
ALTER TABLE r2_retirement_writer_fences ENABLE TRIGGER r2_writer_fence_transition;
