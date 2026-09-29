-- A claim token exists only while an event is PENDING. MarkOutboxPublished relies on it to
-- leave status out of its WHERE clause (see outbox_events.sql). NOT VALID takes the lock only for
-- the catalogue change; 000021 validates existing rows without blocking writes.
ALTER TABLE outbox_events
    ADD CONSTRAINT chk_outbox_claim_pending CHECK (claim_token IS NULL OR status = 'PENDING') NOT VALID;
