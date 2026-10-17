ALTER TABLE events ADD COLUMN payload_hash TEXT;

-- Backfill existing rows (md5 of the jsonb text representation).
UPDATE events SET payload_hash = md5(payload::text);

ALTER TABLE events ALTER COLUMN payload_hash SET NOT NULL;
