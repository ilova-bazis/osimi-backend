ALTER TABLE object_events
  ADD COLUMN IF NOT EXISTS event_object_id text;
