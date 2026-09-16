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

Exits 0 on success, 1 on failure. Designed to run to completion and exit.

## Reliability and security

- JSON/OMM elements are the source of truth. The worker does not fetch,
  derive, or require TLE text. Existing TLE columns are left untouched.
- Each provider group is fetched once, with a 60-second deadline. Any non-200
  response stops the entire run; no redirects or provider retries are followed.
- Database writes use batches of 100. Statement timeouts split idempotent
  writes into smaller requests; retries of singleton writes are bounded.
- Identifier lookups paginate and fail closed on errors. Identifier inserts
  are not blindly retried. Keep one worker replica; concurrent writers require
  a database uniqueness constraint for active identifiers.
- A 15-minute process deadline prevents hung requests blocking future jobs.
- Node 24 and the Supabase dependency are pinned. Use `npm ci --ignore-scripts`.
- `npm test` uses mocked services and never reads production credentials.
- Railway builds run the tests; failing tests prevent deployment.
- Server credentials belong only in Railway variables. `SUPABASE_SECRET_KEY`
  is supported, with `SUPABASE_SERVICE_ROLE_KEY` retained as a fallback.
  Both are privileged; neither belongs in Git or a browser client.
