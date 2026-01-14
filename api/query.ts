import type { VercelRequest, VercelResponse } from "@vercel/node";

const API_VERSION = "2026-01-14_volume_kpi_v2";

// ✅ 1. Define the Data Contract
type KpiRequestV1 = {
  contract_version: "kpi_request.v1";
  kpi: "volume" | "revenue" | "share"; // expandable
  filters?: {
    megabrand?: string[];
    wholesaler_id?: string[];
    channel?: string[];
  };
};

function setCors(req: VercelRequest, res: VercelResponse) {
  const origin = req.headers.origin || "";

  // ✅ Security: Allow Localhost + Your Production Domains
  const isLocalhost =
    origin.startsWith("http://localhost") ||
    origin.startsWith("https://localhost") ||
    origin.startsWith("http://127.0.0.1");

  const allowedDomains = ["https://brickhouser3.github.io"];

  if (isLocalhost || allowedDomains.includes(origin)) {
    res.setHeader("Access-Control-Allow-Origin", origin);
  }

  res.setHeader("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
  res.setHeader(
    "Access-Control-Allow-Headers",
    "Content-Type, Authorization, Accept, x-mc-api, x-mc-version"
  );
  
  // Expose headers so your frontend can see debug info
  res.setHeader(
    "Access-Control-Expose-Headers",
    "x-mc-api, x-mc-origin, x-mc-version, Content-Length, Access-Control-Allow-Origin"
  );

  res.setHeader("Access-Control-Max-Age", "86400");
}

function sleep(ms: number) {
  return new Promise((r) => setTimeout(r, ms));
}

export default async function handler(req: VercelRequest, res: VercelResponse) {
  setCors(req, res);

  // Handle Preflight
  if (req.method === "OPTIONS") return res.status(200).end();

  // Debug Headers
  res.setHeader("x-mc-api", "query.ts");
  res.setHeader("x-mc-version", API_VERSION);

  try {
    if (req.method === "GET") {
      return res.status(200).json({
        ok: true,
        status: "operational",
        version: API_VERSION,
        note: "Use POST to query KPIs"
      });
    }

    if (req.method !== "POST") {
      return res.status(405).json({ ok: false, error: "Method not allowed" });
    }

    // Parse Body
    const body = typeof req.body === "string" ? JSON.parse(req.body || "{}") : req.body;

    // Ping / Transport Check
    if (body?.ping === true) {
      return res.status(200).json({
        ok: true,
        mode: "ping",
        version: API_VERSION
      });
    }

    // --- DATABRICKS CONNECTION CHECKS ---
    const host = process.env.DATABRICKS_HOST;
    const token = process.env.DATABRICKS_TOKEN;
    const warehouseId = process.env.WAREHOUSE_ID;

    if (!host || !token || !warehouseId) {
      return res.status(500).json({
        ok: false,
        error: "Server missing Databricks credentials",
        version: API_VERSION
      });
    }

    const { kpi, filters } = body as KpiRequestV1;

    // ✅ 2. DYNAMIC SQL BUILDER
    let finalSql = "";
    
    // Default Base Table (The one you just created)
    const volumeTable = "commercial_dev.capabilities.mbmc_actuals_volume";

    if (kpi === "volume") {
      // Build WHERE Clause dynamically
      const conditions: string[] = ["1=1"]; // Default true prevents syntax errors if no filters

      if (filters?.megabrand && filters.megabrand.length > 0) {
        // Safe string formatting for SQL IN clause
        const list = filters.megabrand.map(s => `'${s.replace(/'/g, "''")}'`).join(",");
        conditions.push(`megabrand IN (${list})`);
      }

      if (filters?.wholesaler_id && filters.wholesaler_id.length > 0) {
        const list = filters.wholesaler_id.map(s => `'${s.replace(/'/g, "''")}'`).join(",");
        conditions.push(`WSLR_NBR IN (${list})`);
      }

      if (filters?.channel && filters.channel.length > 0) {
        const list = filters.channel.map(s => `'${s.replace(/'/g, "''")}'`).join(",");
        conditions.push(`channel IN (${list})`);
      }

      // Construct Final SQL
      // Aggregating by Month (Time Series) is the standard default for charts
      finalSql = `
        SELECT 
          cal_yr_mo_nbr,
          SUM(BBLs) as bbls_cy,
          SUM(BBLs_LY) as bbls_ly
        FROM ${volumeTable}
        WHERE ${conditions.join(" AND ")}
        GROUP BY cal_yr_mo_nbr
        ORDER BY cal_yr_mo_nbr
      `;
    } 
    else {
      // Fallback or Error for unknown KPIs
      return res.status(400).json({ 
        ok: false, 
        error: `KPI '${kpi}' is not yet implemented.` 
      });
    }

    // --- EXECUTE ON DATABRICKS ---
    const submitRes = await fetch(`${host}/api/2.0/sql/statements`, {
      method: "POST",
      headers: {
        Authorization: `Bearer ${token}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        statement: finalSql,
        warehouse_id: warehouseId,
      }),
    });

    const submitted = await submitRes.json();

    if (!submitRes.ok) {
      return res.status(submitRes.status).json({
        ok: false,
        error: "Databricks submit failed",
        dbx_msg: submitted?.message,
        sql_debug: finalSql // Helpful for debugging!
      });
    }

    const statementId = submitted?.statement_id;
    if (!statementId) throw new Error("No statement_id returned");

    // --- POLLING LOOP ---
    const deadlineMs = Date.now() + 15_000; // 15s timeout
    let last = submitted;

    while (Date.now() < deadlineMs) {
      const state = last?.status?.state;
      if (state === "SUCCEEDED" || state === "FAILED" || state === "CANCELED") break;
      
      await sleep(350); // fast polling
      
      const pollRes = await fetch(`${host}/api/2.0/sql/statements/${statementId}`, {
        method: "GET",
        headers: { Authorization: `Bearer ${token}` }
      });
      last = await pollRes.json();
    }

    if (last?.status?.state !== "SUCCEEDED") {
      return res.status(502).json({
        ok: false,
        error: "Query timed out or failed",
        state: last?.status?.state,
        sql_debug: finalSql
      });
    }

    // ✅ Success Return
    return res.status(200).json({
      ok: true,
      result: last.result,
      version: API_VERSION,
      meta: {
        sql_generated: finalSql // Return this so you can verify the query logic in your UI
      }
    });

  } catch (err: any) {
    console.error("API Crash:", err);
    return res.status(500).json({
      ok: false,
      error: "Internal Server Error",
      details: err.message
    });
  }
}