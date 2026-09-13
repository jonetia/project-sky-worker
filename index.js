// Project Sky -- orbital ingestion worker.
//
// Runs on Railway as a scheduled job, not a Supabase edge function.
// CelesTrak's 'active' group is large (historically 10,000+ objects),
// large enough that plain JS processing of it exceeded Supabase edge
// functions' per-invocation CPU budget -- confirmed directly on day one
// of this project. A normal Node process has no such cap, which is
// exactly why the north star called for a small persistent worker to
// carry sustained/bulk work instead of an edge function.
//
// Same identity -> identifiers -> orbital_elements write pattern already
// proven correct on the smaller 'stations' edge function, just able to
// handle catalogue scale. Processes in batches so a single oversized
// request never goes to Supabase, and so progress is visible in logs
// while a large group is still running.

import { createClient } from "@supabase/supabase-js";

const CELESTRAK_BASE = "https://celestrak.org/NORAD/elements/gp.php";
const EARTH_RADIUS_KM = 6378.137; // WGS84 equatorial radius
const MU_EARTH_KM3_S2 = 398600.4418; // Earth's standard gravitational parameter
const BATCH_SIZE = 1000;
const FETCH_TIMEOUT_MS = 20000;

// Comma-separated list, e.g. "active,stations". Widening coverage later
// is a Railway environment variable change, not a code change.
const GROUPS = (process.env.CELESTRAK_GROUPS || "active")
  .split(",")
  .map((g) => g.trim())
  .filter(Boolean);

/** Semi-major axis from mean motion (Kepler's third law), then perigee/apogee altitude. */
function computePerigeeApogeeKm(meanMotionRevPerDay, eccentricity) {
  const nRadPerSec = (meanMotionRevPerDay * 2 * Math.PI) / 86400;
  const aKm = Math.cbrt(MU_EARTH_KM3_S2 / (nRadPerSec * nRadPerSec));
  return {
    perigeeKm: aKm * (1 - eccentricity) - EARTH_RADIUS_KM,
    apogeeKm: aKm * (1 + eccentricity) - EARTH_RADIUS_KM,
  };
}

/** CelesTrak epoch strings are UTC without an explicit offset. Force it, don't assume it. */
function normalizeEpoch(epoch) {
  return epoch.endsWith("Z") ? epoch : `${epoch}Z`;
}

function chunk(arr, size) {
  const out = [];
  for (let i = 0; i < arr.length; i += size) out.push(arr.slice(i, i + size));
  return out;
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/**
 * Real TLE text lines, fetched directly from CelesTrak rather than
 * reconstructed by hand -- generating fixed-width TLE text ourselves
 * risks subtle, hard-to-catch formatting errors that would silently
 * corrupt propagation. Matched to objects by NORAD ID *parsed* from the
 * text (a safe read of a fixed 5-digit field), not by response order.
 * Same function already proven correct on the 'stations' edge function.
 *
 * A short delay before this call (see caller) avoids back-to-back large
 * requests to CelesTrak from the same source -- confirmed by testing:
 * fetching TLE format immediately after JSON format for the 'active'
 * group returned HTTP 403, while the JSON fetch itself succeeded. Likely
 * anti-abuse rate limiting reacting to burst traffic, not a real block.
 *
 * 2026-09-13: the fixed 4s gap alone stopped being enough -- 403s started
 * recurring, and one run hung outright with no timeout set at all. Added
 * an explicit fetch timeout (a hang should fail fast and loudly, not
 * stall the whole job for minutes) and a real retry-with-backoff on 403
 * specifically, since that status is the rate-limit signal this comment
 * already correctly identified, not a hard block.
 */
async function fetchTleLinesByNorad(group, attempt = 1) {
  const url = `${CELESTRAK_BASE}?GROUP=${group}&FORMAT=tle`;
  const resp = await fetch(url, {
    headers: { "User-Agent": "project-sky-worker/0.1" },
    signal: AbortSignal.timeout(FETCH_TIMEOUT_MS),
  });

  if (!resp.ok) {
    if (resp.status === 403 && attempt < 3) {
      const backoffMs = attempt * 8000; // 8s, then 16s
      console.error(`[${group}] TLE fetch got HTTP 403, retrying in ${backoffMs}ms (attempt ${attempt + 1}/3)`);
      await sleep(backoffMs);
      return fetchTleLinesByNorad(group, attempt + 1);
    }
    console.error(`[${group}] TLE fetch failed: HTTP ${resp.status} -- proceeding without lines`);
    return new Map();
  }

  const text = await resp.text();
  const lines = text.split("\n").map((l) => l.trimEnd()).filter((l) => l.length > 0);
  const result = new Map();

  for (let i = 0; i < lines.length - 1; i++) {
    if (lines[i].startsWith("1 ") && lines[i + 1]?.startsWith("2 ")) {
      const noradId = lines[i].substring(2, 7).trim();
      result.set(noradId, { line1: lines[i], line2: lines[i + 1] });
    }
  }
  return result;
}

async function ingestGroup(supabase, group) {
  const url = `${CELESTRAK_BASE}?GROUP=${group}&FORMAT=json`;
  const resp = await fetch(url, {
    headers: { "User-Agent": "project-sky-worker/0.1 (contact: tim, becoming100/sky POC)" },
    signal: AbortSignal.timeout(FETCH_TIMEOUT_MS),
  });

  if (!resp.ok) {
    // If this is a 5xx, do not retry in a loop -- CelesTrak explicitly
    // asks callers to back off on server errors.
    throw new Error(`${group}: HTTP ${resp.status}`);
  }

  const records = await resp.json();
  console.log(`[${group}] fetched ${records.length} records`);

  const tleLines = await (async () => {
    await sleep(8000); // widened from 4000 -- see fetchTleLinesByNorad comment, 2026-09-13
    return fetchTleLinesByNorad(group);
  })();
  console.log(`[${group}] fetched ${tleLines.size} TLE line pairs`);

  let objectsUpserted = 0;
  let identifiersInserted = 0;
  let elementsWritten = 0;

  const batches = chunk(records, BATCH_SIZE);
  for (let i = 0; i < batches.length; i++) {
    const batch = batches[i];

    // 1. Identity. Real constraint (domain, primary_identifier), safe
    //    for PostgREST's on_conflict.
    const objectRows = batch.map((r) => ({
      domain: "orbital",
      primary_identifier: String(r.NORAD_CAT_ID),
      display_name: r.OBJECT_NAME,
      last_seen_at: new Date().toISOString(),
    }));

    const { data: upsertedObjects, error: objError } = await supabase
      .from("sky_objects")
      .upsert(objectRows, { onConflict: "domain,primary_identifier" })
      .select("id, primary_identifier");

    if (objError) throw new Error(`${group} batch ${i}: sky_objects: ${objError.message}`);
    objectsUpserted += upsertedObjects?.length ?? 0;

    const idByNorad = new Map();
    for (const row of upsertedObjects ?? []) idByNorad.set(row.primary_identifier, row.id);

    // 2. Identifiers. object_identifiers has no unique constraint (by
    //    design -- it's a history table), so check what's current and
    //    insert only what's missing.
    const objectIds = Array.from(idByNorad.values());
    const { data: existingIdentifiers } = await supabase
      .from("object_identifiers")
      .select("object_id, identifier_type")
      .in("object_id", objectIds)
      .is("valid_to", null);

    const existingKeys = new Set(
      (existingIdentifiers ?? []).map((row) => `${row.object_id}:${row.identifier_type}`),
    );

    const identifierRows = batch.flatMap((r) => {
      const objectId = idByNorad.get(String(r.NORAD_CAT_ID));
      if (!objectId) return [];
      const rows = [];
      if (!existingKeys.has(`${objectId}:norad_id`)) {
        rows.push({
          object_id: objectId,
          identifier_type: "norad_id",
          identifier_value: String(r.NORAD_CAT_ID),
          source: "celestrak",
        });
      }
      if (!existingKeys.has(`${objectId}:intl_designator`)) {
        rows.push({
          object_id: objectId,
          identifier_type: "intl_designator",
          identifier_value: r.OBJECT_ID,
          source: "celestrak",
        });
      }
      return rows;
    });

    if (identifierRows.length > 0) {
      const { error: idError } = await supabase.from("object_identifiers").insert(identifierRows);
      if (idError) throw new Error(`${group} batch ${i}: object_identifiers: ${idError.message}`);
      identifiersInserted += identifierRows.length;
    }

    // 3. Orbital element history. Insert-only. ON CONFLICT DO NOTHING on
    //    (object_id, epoch, source) makes re-polling the same epoch a
    //    silent no-op -- this is what makes the worker safe to re-run.
    const elementRows = batch.flatMap((r) => {
      const objectId = idByNorad.get(String(r.NORAD_CAT_ID));
      if (!objectId) return [];
      const { perigeeKm, apogeeKm } = computePerigeeApogeeKm(r.MEAN_MOTION, r.ECCENTRICITY);
      const tle = tleLines.get(String(r.NORAD_CAT_ID));
      return [{
        object_id: objectId,
        epoch: normalizeEpoch(r.EPOCH),
        source: "celestrak",
        line1: tle?.line1 ?? null,
        line2: tle?.line2 ?? null,
        mean_motion: r.MEAN_MOTION,
        eccentricity: r.ECCENTRICITY,
        inclination_deg: r.INCLINATION,
        raan_deg: r.RA_OF_ASC_NODE,
        arg_of_perigee_deg: r.ARG_OF_PERICENTER,
        mean_anomaly_deg: r.MEAN_ANOMALY,
        bstar: r.BSTAR,
        perigee_km: perigeeKm,
        apogee_km: apogeeKm,
        raw: r, // full source record preserved regardless of which fields we parsed above
      }];
    });

    if (elementRows.length > 0) {
      const { data: insertedElements, error: elError } = await supabase
        .from("orbital_elements")
        .upsert(elementRows, { onConflict: "object_id,epoch,source", ignoreDuplicates: true })
        .select("id");
      if (elError) throw new Error(`${group} batch ${i}: orbital_elements: ${elError.message}`);
      elementsWritten += insertedElements?.length ?? 0;
    }

    console.log(
      `[${group}] batch ${i + 1}/${batches.length} done -- objects ${objectsUpserted}, elements ${elementsWritten}`,
    );
  }

  return {
    group,
    recordCount: records.length,
    objectsUpserted,
    identifiersInserted,
    elementsWritten,
    tleLinesFetched: tleLines.size,
    // 2026-09-13: makes a bad TLE fetch visible in the completion log itself
    // instead of only a console.error line -- this is what let the 18:04
    // run's near-total TLE shortfall go unnoticed for 6+ hours.
    tleShortfall: tleLines.size < records.length * 0.5,
  };
}

async function main() {
  const startedAt = Date.now();
  const supabaseUrl = process.env.SUPABASE_URL;
  const serviceRoleKey = process.env.SUPABASE_SERVICE_ROLE_KEY;

  if (!supabaseUrl || !serviceRoleKey) {
    console.error("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY environment variables.");
    process.exit(1);
  }

  const supabase = createClient(supabaseUrl, serviceRoleKey);

  let hadError = false;
  for (const group of GROUPS) {
    try {
      const result = await ingestGroup(supabase, group);
      console.log(`[${group}] complete:`, result);
    } catch (err) {
      hadError = true;
      console.error(`[${group}] failed:`, err.message);
    }
  }

  console.log(`Done in ${Date.now() - startedAt}ms`);
  process.exit(hadError ? 1 : 0);
}

main();
