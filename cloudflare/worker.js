/**
 * proqual-api — Cloudflare Worker
 *
 * DEPLOY: cd cloudflare && wrangler deploy
 *   Køres IKKE af deploy.sh — skal køres manuelt ved ændringer her.
 *   Ved ny kolonne i MAIN_QUERY: brug deploy_all.sh så GitHub Pages også opdateres.
 *
 * SINGLE SOURCE OF TRUTH for SQL — export_static.py henter data herfra
 * i stedet for at have sin egen query-kopi.
 *
 * Proxyer API-kald fra frontend til Turso.
 * Holder TURSO_URL, TURSO_AUTH_TOKEN og API_KEY som Cloudflare Secrets.
 *
 * Endpoints:
 *   GET  /api/properties         — alle salgsboliger med lejedata
 *   GET  /api/annotations        — brugerannotations
 *   POST /api/annotations/:id    — gem annotation
 *   GET  /api/meta               — boligtyper, antal, seneste scrape
 */

// CORS — åben for alle origins da API_KEY er sikkerhedsmekanismen
const CORS_ORIGIN = "*";

// ── Hoved-query (identisk med app.py) ────────────────────────────────────────
const MAIN_QUERY = `
SELECT
    p.id,
    p.address,
    p.zip_code,
    p.city,
    p.price,
    p.size_sqm,
    p.rooms,
    p.property_type,
    p.owner_costs_monthly,
    p.energy_label,
    p.listing_url,
    p.scraped_at,
    CASE WHEN p.size_sqm > 0
         THEN ROUND(CAST(p.price AS REAL) / p.size_sqm, 0)
         ELSE NULL END AS sale_kr_per_sqm,
    ra_zip.price_per_sqm_median AS leje_kr_per_sqm,
    ra_zip.price_per_sqm_low    AS leje_kr_per_sqm_low,
    ra_zip.price_per_sqm_high   AS leje_kr_per_sqm_high,
    CASE WHEN p.size_sqm > 0 AND ra_zip.price_per_sqm_median IS NOT NULL
         THEN ROUND(ra_zip.price_per_sqm_median * p.size_sqm, 0)
         ELSE NULL END AS est_leje_sqm,
    CASE WHEN p.size_sqm > 0 AND ra_zip.price_per_sqm_low IS NOT NULL
         THEN ROUND(ra_zip.price_per_sqm_low * p.size_sqm, 0)
         ELSE NULL END AS est_leje_sqm_low,
    CASE WHEN p.size_sqm > 0 AND ra_zip.price_per_sqm_high IS NOT NULL
         THEN ROUND(ra_zip.price_per_sqm_high * p.size_sqm, 0)
         ELSE NULL END AS est_leje_sqm_high,
    CASE WHEN p.price > 0 AND p.size_sqm > 0 AND ra_zip.price_per_sqm_median IS NOT NULL
         THEN ROUND((ra_zip.price_per_sqm_median * p.size_sqm * 12.0) / p.price * 100, 1)
         ELSE NULL END AS yield_sqm_pct,
    ra_zip.sample_size AS lejedata_antal,
    p.distance_km,
    p.commute_minutes,
    p.days_on_market,
    p.price_change_count,
    p.price_change_amount,
    ra_rooms.rent_total_median AS est_leje_rooms,
    ra_rooms.rent_total_low    AS est_leje_rooms_low,
    ra_rooms.rent_total_high   AS est_leje_rooms_high,
    ra_rooms.sample_size       AS lejedata_rum_antal,
    CASE WHEN p.price > 0 AND ra_rooms.rent_total_median IS NOT NULL
         THEN ROUND((ra_rooms.rent_total_median * 12.0) / p.price * 100, 1)
         ELSE NULL END AS yield_rooms_pct,
    -- V2-model: forventet_leje = 4252 + b_zip × size_sqm
    CASE WHEN p.size_sqm > 0
         THEN ROUND(4252.0 + COALESCE(zr.b_zip,
              (SELECT AVG(b_zip) FROM rental_zip_rates)) * p.size_sqm, 0)
         ELSE NULL END AS est_leje_v2,
    CASE WHEN p.price > 0 AND p.size_sqm > 0
         THEN ROUND((4252.0 + COALESCE(zr.b_zip,
              (SELECT AVG(b_zip) FROM rental_zip_rates)) * p.size_sqm) * 12.0 / p.price * 100, 1)
         ELSE NULL END AS yield_v2_pct,
    COALESCE(zr.sample_size, 0) AS lejedata_v2_antal
FROM properties_for_sale p
LEFT JOIN rental_aggregates ra_zip
    ON ra_zip.zip_code = p.zip_code
    AND ra_zip.rooms IS NULL
    AND ra_zip.property_type IS NULL
LEFT JOIN rental_aggregates ra_rooms
    ON ra_rooms.zip_code = p.zip_code
    AND ra_rooms.rooms = p.rooms
    AND ra_rooms.property_type IS NULL
LEFT JOIN rental_zip_rates zr
    ON zr.zip_code = p.zip_code
WHERE p.price IS NOT NULL
  AND (p.is_active IS NULL OR p.is_active = 1)
ORDER BY yield_v2_pct DESC NULLS LAST, yield_sqm_pct DESC NULLS LAST
`;

// ── CORS ──────────────────────────────────────────────────────────────────────
function getCorsHeaders() {
  return {
    "Access-Control-Allow-Origin": CORS_ORIGIN,
    "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type, X-API-Key",
  };
}

function json(data, status = 200, extraHeaders = {}) {
  return new Response(JSON.stringify(data), {
    status,
    headers: { "Content-Type": "application/json", ...extraHeaders },
  });
}

// ── Turso HTTP API ────────────────────────────────────────────────────────────
async function tursoExecute(env, sql, args = []) {
  // libsql:// → https://
  const baseUrl = env.TURSO_URL.replace("libsql://", "https://");
  const endpoint = `${baseUrl}/v2/pipeline`;

  const res = await fetch(endpoint, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${env.TURSO_AUTH_TOKEN}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      requests: [
        { type: "execute", stmt: { sql, args } },
        { type: "close" },
      ],
    }),
  });

  if (!res.ok) {
    const text = await res.text();
    throw new Error(`Turso HTTP ${res.status}: ${text}`);
  }

  const data = await res.json();
  const result = data.results?.[0];

  if (!result || result.type === "error") {
    throw new Error(`SQL fejl: ${result?.error?.message ?? "ukendt"}`);
  }

  const { cols, rows } = result.response.result;
  const colNames = cols.map((c) => c.name);

  // Turso returnerer værdier som {type, value}-objekter
  return rows.map((row) => {
    const obj = {};
    colNames.forEach((name, i) => {
      const cell = row[i];
      if (cell === null || cell?.type === "null") {
        obj[name] = null;
      } else if (typeof cell === "object" && "value" in cell) {
        // Konverter tal-strenge til tal
        const v = cell.value;
        obj[name] =
          (cell.type === "integer" || cell.type === "float") && v !== null
            ? Number(v)
            : v;
      } else {
        obj[name] = cell;
      }
    });
    return obj;
  });
}

// ── Request handler ───────────────────────────────────────────────────────────
export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const corsHeaders = getCorsHeaders();

    // CORS preflight
    if (request.method === "OPTIONS") {
      return new Response(null, { status: 204, headers: corsHeaders });
    }

    const responseHeaders = {
      ...corsHeaders,
      "Content-Type": "application/json",
    };

    // Valider API-nøgle
    const apiKey = request.headers.get("X-API-Key");
    if (!apiKey || apiKey !== env.API_KEY) {
      return new Response(JSON.stringify({ error: "Unauthorized" }), {
        status: 401,
        headers: responseHeaders,
      });
    }

    try {
      const path = url.pathname;

      // ── GET /api/properties ─────────────────────────────────────────────
      if (path === "/api/properties" && request.method === "GET") {
        const rows = await tursoExecute(env, MAIN_QUERY);
        return new Response(JSON.stringify(rows), { headers: responseHeaders });
      }

      // ── GET /api/annotations ────────────────────────────────────────────
      if (path === "/api/annotations" && request.method === "GET") {
        let rows;
        try {
          rows = await tursoExecute(env, "SELECT * FROM property_annotations");
        } catch {
          // Tabellen eksisterer ikke endnu — returner tom dict
          return new Response(JSON.stringify({}), { headers: responseHeaders });
        }
        const result = {};
        for (const r of rows) {
          try {
            r.renovation_items = JSON.parse(r.renovation_items || "[]");
          } catch {
            r.renovation_items = [];
          }
          result[r.property_id] = r;
        }
        return new Response(JSON.stringify(result), { headers: responseHeaders });
      }

      // ── POST /api/annotations/:id ───────────────────────────────────────
      const annotMatch = path.match(/^\/api\/annotations\/(\d+)$/);
      if (annotMatch && request.method === "POST") {
        const propertyId = parseInt(annotMatch[1], 10);
        const data = await request.json();
        const renovationItems = JSON.stringify(data.renovation_items || []);

        // Opret tabel hvis den ikke eksisterer
        await tursoExecute(env, `
          CREATE TABLE IF NOT EXISTS property_annotations (
            property_id         INTEGER PRIMARY KEY,
            status              TEXT    DEFAULT 'neutral',
            renovation_items    TEXT    DEFAULT '[]',
            renovation_status   TEXT    DEFAULT 'none',
            notes               TEXT    DEFAULT '',
            custom_leje         REAL    DEFAULT NULL,
            custom_leje_note    TEXT    DEFAULT NULL,
            custom_price        REAL    DEFAULT NULL,
            custom_price_note   TEXT    DEFAULT NULL,
            updated_at          TEXT    DEFAULT (datetime('now'))
          )
        `);

        // Migrér eksisterende tabel (ignorér fejl hvis kolonner allerede findes)
        for (const colDef of [
          "custom_leje REAL DEFAULT NULL",
          "custom_leje_note TEXT DEFAULT NULL",
          "custom_price REAL DEFAULT NULL",
          "custom_price_note TEXT DEFAULT NULL",
        ]) {
          try { await tursoExecute(env, `ALTER TABLE property_annotations ADD COLUMN ${colDef}`); } catch {}
        }

        await tursoExecute(
          env,
          `INSERT INTO property_annotations
              (property_id, status, renovation_items, renovation_status, notes,
               custom_leje, custom_leje_note, custom_price, custom_price_note, updated_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
           ON CONFLICT(property_id) DO UPDATE SET
              status              = excluded.status,
              renovation_items    = excluded.renovation_items,
              renovation_status   = excluded.renovation_status,
              notes               = excluded.notes,
              custom_leje         = excluded.custom_leje,
              custom_leje_note    = excluded.custom_leje_note,
              custom_price        = excluded.custom_price,
              custom_price_note   = excluded.custom_price_note,
              updated_at          = excluded.updated_at`,
          [
            { type: "integer", value: String(propertyId) },
            { type: "text",    value: data.status || "neutral" },
            { type: "text",    value: renovationItems },
            { type: "text",    value: data.renovation_status || "none" },
            { type: "text",    value: data.notes || "" },
            data.custom_leje != null
              ? { type: "float", value: Number(data.custom_leje) }
              : { type: "null" },
            { type: "text", value: data.custom_leje_note  || "" },
            data.custom_price != null
              ? { type: "float", value: Number(data.custom_price) }
              : { type: "null" },
            { type: "text", value: data.custom_price_note || "" },
          ]
        );

        return new Response(JSON.stringify({ ok: true }), { headers: responseHeaders });
      }

      // ── GET /api/meta ───────────────────────────────────────────────────
      if (path === "/api/meta" && request.method === "GET") {
        const [types, countRows, scrapedRows] = await Promise.all([
          tursoExecute(
            env,
            "SELECT DISTINCT property_type FROM properties_for_sale WHERE property_type IS NOT NULL ORDER BY property_type"
          ),
          tursoExecute(
            env,
            "SELECT COUNT(*) AS n FROM properties_for_sale WHERE price IS NOT NULL AND (is_active IS NULL OR is_active = 1)"
          ),
          tursoExecute(
            env,
            "SELECT MAX(scraped_at) AS ts FROM properties_for_sale"
          ),
        ]);

        return new Response(
          JSON.stringify({
            property_types: types.map((r) => r.property_type),
            total: countRows[0]?.n ?? 0,
            scraped_at: scrapedRows[0]?.ts ?? null,
          }),
          { headers: responseHeaders }
        );
      }

      return new Response(JSON.stringify({ error: "Not found" }), {
        status: 404,
        headers: responseHeaders,
      });
    } catch (err) {
      return new Response(JSON.stringify({ error: err.message }), {
        status: 500,
        headers: responseHeaders,
      });
    }
  },
};
