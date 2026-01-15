import type { VercelRequest, VercelResponse } from "@vercel/node";

const API_VERSION = "2026-01-15_global_filters_v1";

type KpiRequestV1 = {
  contract_version: "kpi_request.v1";
  kpi: "volume" | "revenue" | "share";
  groupBy?: "time" | "megabrand";
  max_month?: string; // e.g. "202510"
  scope?: "MTD" | "YTD"; // e.g. "YTD"
  filters?: {
    megabrand?: string[];
    wholesaler_id?: string[];
    channel?: string[];
  };
};

function setCors(req: VercelRequest, res: VercelResponse) {
  const origin = req.headers.origin || "";
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
  if (req.method === "OPTIONS") return res.status(200).end();

  res.setHeader("x-mc-api", "query.ts");
  res.setHeader("x-mc-version", API_VERSION);

  try {
    if (req.method === "GET") {
      return res.status(200).json({
        ok: true,
        status: "operational",
        version: API_VERSION,
      });
    }

    if (req.method !== "POST") {
      return res.status(405).json({ ok: false, error: "Method not allowed" });
    }

    const body =
      typeof req.body === "string" ? JSON.parse(req.body || "{}") : req.body;

    if (body?.ping === true) {
      return res.status(200).json({ ok: true, mode: "ping" });
    }

    // --- CREDENTIALS ---
    const host = process.env.DATABRICKS_HOST;
    const token = process.env.DATABRICKS_TOKEN;
    const warehouseId = process.env.WAREHOUSE_ID;

    if (!host || !token || !warehouseId) {
      return res.status(500).json({
        ok: false,
        error: "Server missing Databricks credentials",
      });
    }

    const {
      kpi,
      filters,
      groupBy = "time",
      max_month = "202512",
      scope = "YTD",
    } = body as KpiRequestV1;

    // --- TABLE SELECTION ---
    let table = "";
    if (kpi === "volume") table = "commercial_dev.capabilities.mbmc_actuals_volume";
    else if (kpi === "revenue") table = "commercial_dev.capabilities.mbmc_actuals_revenue";
    else {
      return res.status(400).json({
        ok: false,
        error: `KPI '${kpi}' is not yet implemented.`,
      });
    }

    // --- FILTER LOGIC ---
    const conditions: string[] = ["1=1"];

    // ✅ TIME SCOPE LOGIC
    if (scope === "MTD") {
      // Strict equality: Just the selected month
      conditions.push(`cal_yr_mo_nbr = ${max_month}`);
    } else {
      // YTD: From Jan 1st of that year up to max_month
      const startOfYear = max_month.substring(0, 4) + "01";
      conditions.push(`cal_yr_mo_nbr BETWEEN ${startOfYear} AND ${max_month}`);
    }

    // ✅ BRAND/DIMENSION FILTERS
    if (filters?.megabrand && filters.megabrand.length > 0) {
      const list = filters.megabrand
        .map((s) => `'${s.replace(/'/g, "''")}'`)
        .join(",");
      conditions.push(`megabrand IN (${list})`);
    }

    // --- DYNAMIC GROUPING ---
    let selectClause = "";
    let groupByClause = "";
    let orderByClause = "";
    const col = kpi === "revenue" ? "REV" : "BBLs";

    if (groupBy === "megabrand") {
      // BRAND RANKING
      selectClause = `
        megabrand as dimension,
        SUM(${col}) as val_cy,
        SUM(${col}_LY) as val_ly
      `;
      groupByClause = "GROUP BY megabrand";
      orderByClause = "ORDER BY val_cy DESC";
    } else {
      // TIME SERIES DEFAULT
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
        sql: finalSql,
      });
    }

    const statementId = submitted?.statement_id;
    if (!statementId) throw new Error("No statement_id returned");

    // --- POLLING LOOP ---
    const deadlineMs = Date.now() + 15000;
    let last = submitted;

    while (Date.now() < deadlineMs) {
      const state = last?.status?.state;
      if (["SUCCEEDED", "FAILED", "CANCELED"].includes(state)) break;
      await sleep(350);
      const pollRes = await fetch(
        `${host}/api/2.0/sql/statements/${statementId}`,
        {
          method: "GET",
          headers: { Authorization: `Bearer ${token}` },
        }
      );
      last = await pollRes.json();
    }

    if (last?.status?.state !== "SUCCEEDED") {
      return res.status(502).json({
        ok: false,
        error: "Query timed out or failed",
        state: last?.status?.state,
      });
    }

    return res.status(200).json({
      ok: true,
      result: last.result,
      version: API_VERSION,
      meta: { sql: finalSql },
    });
  } catch (err: any) {
    console.error("API Crash:", err);
    return res.status(500).json({
      ok: false,
      error: "Internal Server Error",
      details: err.message,
    });
  }
}