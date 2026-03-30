# Forward Migrations

This directory is intentionally empty after the v1 bootstrap cutover.

Fresh ClickHouse volumes initialize from [`../bootstrap/v1.sql`](../bootstrap/v1.sql).
Any future schema changes should be added here as forward-only migrations and
applied on top of the bootstrap baseline.

Rules:

- keep filenames lexically ordered
- write only deltas from the bootstrap baseline
- do not recreate the retired historical chain here
- use `make migrate-up` to apply forward migrations to an existing database
