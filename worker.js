// JSON/OMM orbital elements are authoritative. TLE text is neither fetched nor required.
const CELESTRAK_BASE = "https://celestrak.org/NORAD/elements/gp.php";
const BATCH_SIZE = 100;
const PAGE_SIZE = 500;
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));

export function normalizeEpoch(epoch) {
  if (typeof epoch !== "string") throw new Error("Missing orbital epoch");
  const normalized = /(?:Z|[+-]\d{2}:\d{2})$/i.test(epoch) ? epoch : `${epoch}Z`;
  if (!Number.isFinite(Date.parse(normalized))) throw new Error("Invalid orbital epoch");
  return normalized;
}

export function validateRecords(records) {
  if (!Array.isArray(records) || !records.length) throw new Error("Provider returned an empty or invalid catalogue");
  const seen = new Set();
  for (const r of records) {
    if (!r || !Number.isSafeInteger(r.NORAD_CAT_ID) || r.NORAD_CAT_ID <= 0) throw new Error("Invalid NORAD ID");
    if (seen.has(r.NORAD_CAT_ID)) throw new Error("Duplicate NORAD ID in provider response");
    seen.add(r.NORAD_CAT_ID);
    normalizeEpoch(r.EPOCH);
    for (const field of ["MEAN_MOTION", "ECCENTRICITY", "INCLINATION", "RA_OF_ASC_NODE", "ARG_OF_PERICENTER", "MEAN_ANOMALY", "BSTAR"]) {
      if (!Number.isFinite(r[field])) throw new Error(`Invalid orbital field: ${field}`);
    }
    if (r.MEAN_MOTION <= 0 || r.ECCENTRICITY < 0 || r.ECCENTRICITY >= 1) throw new Error("Invalid orbital geometry");
  }
}

function databaseError(label, error) {
  return new Error(`${label}: ${error.code || "request error"}: ${error.message}`);
}

// Replay only operations protected by database conflict keys. A PostgreSQL
// statement timeout rolls back that statement, so split oversized writes.
export async function writeIdempotent(rows, write, label, pause = sleep, attempt = 0) {
  const { data, error } = await write(rows);
  if (!error) return data ?? [];
  if (error.code === "57014" && rows.length > 1) {
    const mid = Math.ceil(rows.length / 2);
    await pause(500);
    return [
      ...await writeIdempotent(rows.slice(0, mid), write, label, pause),
      ...await writeIdempotent(rows.slice(mid), write, label, pause),
    ];
  }
  if (["57014", "40001", "40P01"].includes(error.code) && attempt < 2) {
    await pause(1000 * (attempt + 1));
    return writeIdempotent(rows, write, label, pause, attempt + 1);
  }
  throw databaseError(label, error);
}

export async function readIdentifiers(supabase, objectIds) {
  const rows = [];
  // Keyset pagination also works when the server caps pages below PAGE_SIZE.
  let lastId;
  for (;;) {
    let query = supabase.from("object_identifiers")
      .select("id,object_id,identifier_type,identifier_value")
      .in("object_id", objectIds).is("valid_to", null)
      .order("id").limit(PAGE_SIZE);
    if (lastId) query = query.gt("id", lastId);
    const { data, error } = await query;
    if (error) throw databaseError("identifier lookup", error);
    if (!Array.isArray(data)) throw new Error("Identifier lookup returned no data");
    if (!data.length) return rows;
    const nextId = data.at(-1).id;
    if (!nextId || nextId === lastId) throw new Error("Identifier pagination did not advance");
    rows.push(...data);
    lastId = nextId;
  }
}

export async function ingestGroup(supabase, group, { fetchFn = fetch, pause = sleep, log = console.log } = {}) {
  const url = new URL(CELESTRAK_BASE);
  url.search = new URLSearchParams({ GROUP: group, FORMAT: "json" });
  const response = await fetchFn(url, {
    headers: { "User-Agent": "project-sky-worker/0.2" },
    signal: AbortSignal.timeout(60000), redirect: "manual",
  });
  // Provider policy: stop and report every non-200 response, including redirects.
  if (response.status !== 200) throw new Error(`CelesTrak ${group}: HTTP ${response.status}; no retry attempted`);
  const records = await response.json();
  validateRecords(records);
  log(`[${group}] fetched ${records.length} JSON records`);
  let objectsUpserted = 0, identifiersInserted = 0, elementsWritten = 0;
  for (let offset = 0; offset < records.length; offset += BATCH_SIZE) {
    const batch = records.slice(offset, offset + BATCH_SIZE);
    const now = new Date().toISOString();
    const objects = await writeIdempotent(batch.map(r => ({
      domain: "orbital", primary_identifier: String(r.NORAD_CAT_ID),
      display_name: r.OBJECT_NAME, last_seen_at: now,
    })), rows => supabase.from("sky_objects")
      .upsert(rows, { onConflict: "domain,primary_identifier" }).select("id,primary_identifier"), "sky_objects", pause);
    const idByNorad = new Map(objects.map(r => [r.primary_identifier, r.id]));
    if (batch.some(r => !idByNorad.has(String(r.NORAD_CAT_ID)))) throw new Error("Incomplete sky_objects response; stopped before dependent writes");
    objectsUpserted += objects.length;

    const existing = await readIdentifiers(supabase, [...idByNorad.values()]);
    const key = r => JSON.stringify([r.object_id, r.identifier_type, r.identifier_value]);
    const keys = new Set(existing.map(key));
    const identifiers = batch.flatMap(r => {
      const object_id = idByNorad.get(String(r.NORAD_CAT_ID));
      const candidates = [{ object_id, identifier_type: "norad_id", identifier_value: String(r.NORAD_CAT_ID), source: "celestrak" }];
      if (r.OBJECT_ID) candidates.push({ object_id, identifier_type: "intl_designator", identifier_value: r.OBJECT_ID, source: "celestrak" });
      return candidates.filter(row => !keys.has(key(row)));
    });
    if (identifiers.length) {
      // No blind retries: an ambiguous response could mean this insert committed.
      const { error } = await supabase.from("object_identifiers").insert(identifiers);
      if (error) throw databaseError("object_identifiers", error);
      identifiersInserted += identifiers.length;
    }

    const elements = batch.map(r => {
      const n = r.MEAN_MOTION * 2 * Math.PI / 86400;
      const a = Math.cbrt(398600.4418 / (n * n));
      return {
        object_id: idByNorad.get(String(r.NORAD_CAT_ID)), epoch: normalizeEpoch(r.EPOCH), source: "celestrak",
        mean_motion: r.MEAN_MOTION, eccentricity: r.ECCENTRICITY, inclination_deg: r.INCLINATION,
        raan_deg: r.RA_OF_ASC_NODE, arg_of_perigee_deg: r.ARG_OF_PERICENTER, mean_anomaly_deg: r.MEAN_ANOMALY,
        bstar: r.BSTAR, perigee_km: a * (1 - r.ECCENTRICITY) - 6378.137,
        apogee_km: a * (1 + r.ECCENTRICITY) - 6378.137, raw: r,
      };
    });
    const inserted = await writeIdempotent(elements, rows => supabase.from("orbital_elements")
      .upsert(rows, { onConflict: "object_id,epoch,source", ignoreDuplicates: true }).select("id"), "orbital_elements", pause);
    elementsWritten += inserted.length;
    log(`[${group}] ${offset + batch.length}/${records.length}: objects ${objectsUpserted}, identifiers ${identifiersInserted}, elements ${elementsWritten}`);
  }
  return { group, recordCount: records.length, objectsUpserted, identifiersInserted, elementsWritten };
}
