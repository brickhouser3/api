import type { VercelRequest, VercelResponse } from "@vercel/node";

const API_VERSION = "2026-01-15_brand_matrix_v1";

// ✅ 1. Updated Contract: Added 'groupBy'
type KpiRequestV1 = {
  contract_version: "kpi_request.v1";
  kpi: "volume" | "revenue" | "share"; 
  groupBy?: "time" | "megabrand"; // <--- NEW
  filters?: {
    megabrand?: string[];
    wholesaler_id?: string[];
    channel?: string[];
  };
};

function setCors(req: VercelRequest, res: VercelResponse) {
  const origin = req.headers.origin || "";
  const isLocalhost = origin.startsWith("http://localhost") || origin.startsWith("https://localhost") || origin.startsWith("http://127.0.0.1");
  const allowedDomains = ["https://brickhouser3.github.io"];

  if (isLocalhost || allowedDomains.includes(origin)) {
    res.setHeader("Access-Control-Allow-Origin", origin);
  }
  res.setHeader("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type, Authorization, Accept, x-mc-api, x-mc-version");
  res.setHeader("Access-Control-Expose-Headers", "x-mc-api, x-mc-origin, x-mc-version, Content-Length, Access-Control-Allow-Origin");
  res.setHeader("Access-Control-Max-Age", "86400");
}

function sleep(ms: number) {
  return new Promise((r) => setTimeout(r, ms));
}

export default async function handler(req: VercelRequest, res: VercelResponse) {
  setCors(req, res);
  if (req.method === "OPTIONS") return res.status(200).end();
  
  // Debug Headers
  res.setHeader("x-mc-api", "query.ts");
  res.setHeader("x-mc-version", API_VERSION);

  try {
    if (req.method === "GET") return res.status(200).json({ ok: true, status: "operational", version: API_VERSION });
    if (req.method !== "POST") return res.status(405).json({ ok: false, error: "Method not allowed" });

    const body = typeof req.body === "string" ? JSON.parse(req.body || "{}") : req.body;
    if (body?.ping === true) return res.status(200).json({ ok: true, mode: "ping" });

    // --- CREDENTIALS ---
    const host = process.env.DATABRICKS_HOST;
    const token = process.env.DATABRICKS_TOKEN;
    const warehouseId = process.env.WAREHOUSE_ID;

    if (!host || !token || !warehouseId) return res.status(500).json({ ok: false, error: "Missing DBX Creds" });

    const { kpi, filters, groupBy = "time" } = body as KpiRequestV1;

    // --- TABLE SELECTION ---
    let table = "";
    if (kpi === "volume") table = "commercial_dev.capabilities.mbmc_actuals_volume";
    else if (kpi === "revenue") table = "commercial_dev.capabilities.mbmc_actuals_revenue";
    else return res.status(400).json({ ok: false, error: `KPI '${kpi}' not implemented.` });

    // --- FILTER LOGIC ---
    const conditions: string[] = ["1=1"];
    if (filters?.megabrand && filters.megabrand.length > 0) {
      const list = filters.megabrand.map(s => `'${s.replace(/'/g, "''")}'`).join(",");
      conditions.push(`megabrand IN (${list})`);
    }
    // (Add other filters as needed)

    // --- ✅ DYNAMIC GROUPING LOGIC ---
    let selectClause = "";
    let groupByClause = "";
    let orderByClause = "";

    if (groupBy === "megabrand") {
      // BRAND RANKING QUERY
      // We sum up the whole selected period (e.g. YTD if date filters applied, or Full Year if not)
      // Note: We map SUM(REV) or SUM(BBLs) to generic 'val_cy' aliases
      const col = kpi === "revenue" ? "REV" : "BBLs";
      
      selectClause = `
        megabrand as dimension,
        SUM(${col}) as val_cy,
        SUM(${col}_LY) as val_ly
      `;
      groupByClause = "GROUP BY megabrand";
      orderByClause = "ORDER BY val_cy DESC";
    } else {
      // DEFAULT TIME SERIES (Month)
      const col = kpi === "revenue" ? "REV" : "BBLs";

      selectClause = `
        cal_yr_mo_nbr as dimension,
        SUM(${col}) as val_cy,
        SUM(${col}_LY) as val_ly
      `;
      groupByClause = "GROUP BY cal_yr_mo_nbr";
      orderByClause = "ORDER BY cal_yr_mo_nbr";
    }

    const finalSql = `
      SELECT ${selectClause}
      FROM ${table}
      WHERE ${conditions.join(" AND ")}
      ${groupByClause}
      ${orderByClause}
      LIMIT 1000
    `;

    // --- EXECUTE ---
    const submitRes = await fetch(`${host}/api/2.0/sql/statements`, {
      method: "POST",
      headers: { Authorization: `Bearer ${token}`, "Content-Type": "application/json" },
      body: JSON.stringify({ statement: finalSql, warehouse_id: warehouseId }),
    });

    const submitted = await submitRes.json();
    if (!submitRes.ok) return res.status(submitRes.status).json({ ok: false, error: submitted?.message, sql: finalSql });

    // --- POLLING ---
    const statementId = submitted?.statement_id;
    const deadlineMs = Date.now() + 15000;
    let last = submitted;

    while (Date.now() < deadlineMs) {
      if (["SUCCEEDED", "FAILED", "CANCELED"].includes(last?.status?.state)) break;
      await sleep(350);
      const pollRes = await fetch(`${host}/api/2.0/sql/statements/${statementId}`, {
        method: "GET",
        headers: { Authorization: `Bearer ${token}` }
      });
      last = await pollRes.json();
    }

    if (last?.status?.state !== "SUCCEEDED") {
      return res.status(502).json({ ok: false, error: "Timeout/Fail", state: last?.status?.state });
    }

    return res.status(200).json({ ok: true, result: last.result, version: API_VERSION, meta: { sql: finalSql } });

  } catch (err: any) {
    console.error(err);
    return res.status(500).json({ ok: false, error: err.message });
  }
}