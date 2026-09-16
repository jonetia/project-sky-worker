import { createClient } from "@supabase/supabase-js";
import { ingestGroup } from "./worker.js";

const startedAt = Date.now();
const secret = process.env.SUPABASE_SECRET_KEY || process.env.SUPABASE_SERVICE_ROLE_KEY;
const redact = message => secret ? String(message).replaceAll(secret, "[REDACTED]") : String(message);
const watchdog = setTimeout(() => {
  console.error("Worker exceeded its 15-minute deadline");
  process.exit(1);
}, 15 * 60 * 1000);
try {
  const url = process.env.SUPABASE_URL;
  if (!url || !secret) throw new Error("Missing Supabase URL or server credential");
  const groups = [...new Set((process.env.CELESTRAK_GROUPS || "active").split(",").map(g => g.trim()).filter(Boolean))];
  if (!groups.length || groups.some(g => !/^[a-z0-9_-]+$/i.test(g))) throw new Error("Invalid CELESTRAK_GROUPS");
  const supabase = createClient(url, secret, {
    auth: { persistSession: false, autoRefreshToken: false, detectSessionInUrl: false },
  });
  // Stop the entire run on a provider error; do not query other groups or retry it.
  for (const group of groups) console.log(`[${group}] complete:`, await ingestGroup(supabase, group));
} catch (error) {
  console.error("Worker failed:", redact(error.message));
  process.exitCode = 1;
} finally {
  clearTimeout(watchdog);
  console.log(`Done in ${Date.now() - startedAt}ms`);
}
