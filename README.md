# project-sky-worker

Orbital catalogue ingestion for Project Sky.

## Why this exists

The same ingestion logic first ran as a Supabase edge function, scoped to
CelesTrak's small `stations` group (dozens of objects). Pulling the full
`active` group (historically 10,000+ objects) exceeded the edge
function's per-invocation CPU budget partway through processing. This
worker runs the identical logic in a normal Node process instead, which
has no such cap, and is meant to run on a schedule (Railway cron), not
continuously.

## What it does

Fetches one or more CelesTrak GP groups, and for each object writes:

- Identity into `sky_objects`
- Historical identifiers into `object_identifiers`
- Every orbital element set into `orbital_elements` (append-only --
  re-running on the same epoch is a safe no-op, never a duplicate)

Perigee and apogee are computed from the real orbital elements (Kepler's
third law), not parsed from text.

## Environment variables

Set these in Railway's service Variables tab, not in the repo.

| Variable | Required | Notes |
|---|---|---|
| `SUPABASE_URL` | yes | project-sky's URL |
| `SUPABASE_SERVICE_ROLE_KEY` | yes | service role secret, from Supabase project settings |
| `CELESTRAK_GROUPS` | no | comma-separated, defaults to `active` |

## Running it

```
npm install
npm start
```

Exits 0 on success, 1 if any group failed. Designed to run to completion
and exit -- not a long-running server.
