ALTER TABLE deliveries ADD COLUMN replay_of UUID REFERENCES deliveries (id);
ALTER TABLE deliveries DROP CONSTRAINT uq_deliveries_event_sub;
CREATE UNIQUE INDEX uq_deliveries_event_sub_active
    ON deliveries (event_id, subscription_id)
    WHERE replay_of IS NULL;
