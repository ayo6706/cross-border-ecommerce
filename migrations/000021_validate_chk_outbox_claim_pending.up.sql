-- Separate from 000020 so the validating scan runs under SHARE UPDATE EXCLUSIVE, not the ACCESS
-- EXCLUSIVE lock ADD CONSTRAINT held until its transaction ended.
ALTER TABLE outbox_events VALIDATE CONSTRAINT chk_outbox_claim_pending;
