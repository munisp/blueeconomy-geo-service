-- 0019_port_queue_observations_grant: close the migration-chain integrity
-- gap (H2). port_queue_observations was created in 0013 WITHOUT any GRANT
-- to the application role `geo` (role geo is NOSUPERUSER, 0001), so on a
-- fresh 0001→0018 build Store.QueueObservations / InsertQueueObservation
-- and the congestion forecast read path failed with `permission denied for
-- table port_queue_observations` (surfaced as a misleading 503
-- QUEUE_STORE_UNAVAILABLE). Sibling tables carry the grant inline
-- (geofence_transition_events 0013, ais_positions 0002,
-- recommendation_log 0018).
--
-- No-RLS posture (documented, same doctrine as ais_positions and
-- recommendation_log): the queue plane is a shared national picture with
-- no tenant column, so no tenant RLS applies; the application role `geo`
-- holds row-agnostic SELECT/INSERT and nothing else — observations are
-- append-only, geo_ingest has no access.

GRANT SELECT, INSERT ON port_queue_observations TO geo;
