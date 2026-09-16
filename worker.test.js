import test from "node:test";
import assert from "node:assert/strict";
import { createClient } from "@supabase/supabase-js";
import { ingestGroup, normalizeEpoch, writeIdempotent } from "../worker.js";

const record = (id = 100001) => ({ NORAD_CAT_ID: id, OBJECT_NAME: "TEST", OBJECT_ID: "2026-001A", EPOCH: "2026-09-16T00:00:00", MEAN_MOTION: 15, ECCENTRICITY: 0.001, INCLINATION: 53, RA_OF_ASC_NODE: 12, ARG_OF_PERICENTER: 34, MEAN_ANOMALY: 56, BSTAR: 0.0001 });
const json = (data, status = 200) => new Response(JSON.stringify(data), { status, headers: { "Content-Type": "application/json" } });
function database({ lookupError = false, truncatedObjects = false, pageCap = 1, insertError = false } = {}) {
  const identifiers = [], elements = new Map(), calls = [];
  let sequence = 0;
  const client = createClient("https://example.supabase.co", "test-key", { auth: { persistSession: false }, global: { fetch: async (input, options = {}) => {
    const url = new URL(input);
    const table = url.pathname.split("/").at(-1);
    const rows = options.body ? JSON.parse(options.body) : null;
    calls.push({ table, method: options.method, rows, url });
    if (table === "sky_objects") return json(truncatedObjects ? [] : rows.map(r => ({ id: `object-${r.primary_identifier}`, primary_identifier: r.primary_identifier })));
    if (table === "object_identifiers" && !rows) {
      if (lookupError) return json({ code: "57014", message: "statement timeout" }, 500);
      const after = url.searchParams.get("id")?.slice(3);
      return json(identifiers.filter(r => !after || r.id > after).slice(0, pageCap));
    }
    if (table === "object_identifiers") {
      if (insertError) return json({ code: "XX000", message: "ambiguous insert failure" }, 500);
      identifiers.push(...rows.map(r => ({ ...r, id: String(++sequence).padStart(8, "0") })));
      return new Response(null, { status: 201 });
    }
    if (table === "orbital_elements") {
      const inserted = [];
      for (const r of rows) {
        const key = `${r.object_id}:${r.epoch}:${r.source}`;
        if (!elements.has(key)) { elements.set(key, r); inserted.push({ id: key }); }
      }
      return json(inserted);
    }
    throw new Error(`Unexpected database table ${table}`);
  } } });
  return { client, identifiers, elements, calls };
}
const options = records => ({ fetchFn: async url => { assert.equal(url.searchParams.get("FORMAT"), "json"); return json(records); }, pause: async () => {}, log: () => {} });

test("JSON-only ingestion handles six-digit IDs and repeat runs without duplicates", async () => {
  const db = database();
  const first = await ingestGroup(db.client, "active", options([record()]));
  assert.equal(first.identifiersInserted, 2);
  assert.equal(first.elementsWritten, 1);
  const second = await ingestGroup(db.client, "active", options([record()]));
  assert.equal(second.identifiersInserted, 0);
  assert.equal(second.elementsWritten, 0);
  assert.equal(db.identifiers.length, 2);
  const element = [...db.elements.values()][0];
  assert.equal(element.raw.NORAD_CAT_ID, 100001);
  assert.equal("line1" in element, false);
  assert.equal("line2" in element, false);
  assert.ok(element.perigee_km > 0);
});

test("lookup failure stops before identifier and element writes", async () => {
  const db = database({ lookupError: true });
  await assert.rejects(ingestGroup(db.client, "active", options([record()])), /identifier lookup/);
  assert.equal(db.identifiers.length, 0);
  assert.equal(db.elements.size, 0);
});

test("provider errors and redirects are never retried", async () => {
  for (const status of [301, 403, 429, 503]) {
    let calls = 0;
    const db = database();
    await assert.rejects(ingestGroup(db.client, "active", { ...options([]), fetchFn: async (_url, config) => {
      calls++; assert.equal(config.redirect, "manual"); return json({}, status);
    } }), new RegExp(`HTTP ${status}`));
    assert.equal(calls, 1);
    assert.equal(db.calls.length, 0);
  }
});

test("statement timeouts split safe writes and retain all rows", async () => {
  const sizes = [];
  const result = await writeIdempotent([1, 2, 3, 4], async rows => {
    sizes.push(rows.length);
    return rows.length > 1 ? { error: { code: "57014", message: "timeout" } } : { data: rows };
  }, "test", async () => {});
  assert.deepEqual(result, [1, 2, 3, 4]);
  assert.deepEqual(sizes, [4, 2, 1, 1, 2, 1, 1]);
});

test("permanent single-row timeout exhausts bounded retries", async () => {
  let calls = 0;
  await assert.rejects(writeIdempotent([1], async () => { calls++; return { error: { code: "57014", message: "timeout" } }; }, "test", async () => {}), /timeout/);
  assert.equal(calls, 3);
});

test("incomplete object response stops dependent writes", async () => {
  const db = database({ truncatedObjects: true });
  await assert.rejects(ingestGroup(db.client, "active", options([record()])), /Incomplete sky_objects/);
  assert.equal(db.calls.length, 1);
});

test("invalid provider records fail before any writes", async () => {
  for (const records of [[], [record(), record()], [{ ...record(), ECCENTRICITY: 2 }]]) {
    const db = database();
    await assert.rejects(ingestGroup(db.client, "active", options(records)));
    assert.equal(db.calls.length, 0);
  }
});

test("ambiguous identifier inserts are not replayed", async () => {
  const db = database({ insertError: true });
  await assert.rejects(ingestGroup(db.client, "active", options([record()])), /object_identifiers/);
  assert.equal(db.calls.filter(c => c.table === "object_identifiers" && c.rows).length, 1);
});

test("writes are bounded to 100 objects per request", async () => {
  const db = database({ pageCap: 500 });
  const result = await ingestGroup(db.client, "active", options(Array.from({ length: 205 }, (_, i) => record(100001 + i))));
  assert.equal(result.objectsUpserted, 205);
  assert.deepEqual(db.calls.filter(c => c.table === "sky_objects").map(c => c.rows.length), [100, 100, 5]);
});

test("epoch normalization preserves explicit offsets", () => {
  assert.equal(normalizeEpoch("2026-09-16T00:00:00"), "2026-09-16T00:00:00Z");
  assert.equal(normalizeEpoch("2026-09-16T00:00:00+00:00"), "2026-09-16T00:00:00+00:00");
});
