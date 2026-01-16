import type { VercelRequest, VercelResponse } from "@vercel/node";

const API_VERSION = "2026-01-15_filters_v1";

function setCors(req: VercelRequest, res: VercelResponse) {
  const origin = req.headers.origin || "";
  const allowedDomains = ["https://brickhouser3.github.io", "http://localhost:3000"];
  if (allowedDomains.some(d => origin.startsWith(d))) {
    res.setHeader("Access-Control-Allow-Origin", origin);
  }
  res.setHeader("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type, Authorization, x-mc-api, x-mc-version");
}

export default async function handler(req: VercelRequest, res: VercelResponse) {
  setCors(req, res);
  if (req.method === "OPTIONS") return res.status(200).end();

  try {
    const { dimension, table } = typeof req.body === "string" ? JSON.parse(req.body) : req.body;

    // --- SECURITY: Allowlist valid columns/tables to prevent SQL Injection ---
    const ALLOWED_COLS = ["wslr_nbr", "mktng_st_cd", "sls_regn_cd", "channel"];
    const ALLOWED_TABLES = ["mbmc_actuals_volume", "mbmc_actuals_revenue", "mbmc_actuals_distro"];

    if (!ALLOWED_COLS.includes(dimension) || !ALLOWED_TABLES.includes(table)) {
        return res.status(400).json({ ok: false, error: "Invalid dimension or table requested" });
    }

    // --- CREDENTIALS ---
    const host = process.env.DATABRICKS_HOST;
    const token = process.env.DATABRICKS_TOKEN;
    const warehouseId = process.env.WAREHOUSE_ID;

    // --- SQL: Get Distinct Values ---
    // We limit to 2000 to prevent dropdowns from crashing the browser
    const sql = `
      SELECT DISTINCT ${dimension} as label 
      FROM commercial_dev.capabilities.${table} 
      WHERE ${dimension} IS NOT NULL 
      ORDER BY ${dimension} ASC 
      LIMIT 2000
    `;

    // --- EXECUTE ---
    const submitRes = await fetch(`${host}/api/2.0/sql/statements`, {
      method: "POST",
      headers: { Authorization: `Bearer ${token}`, "Content-Type": "application/json" },
      body: JSON.stringify({ statement: sql, warehouse_id: warehouseId }),
    });

    const submitted = await submitRes.json();
    const statementId = submitted?.statement_id;

    // Polling logic (simplified for brevity)
    let state = "PENDING";
    let result = null;
    const start = Date.now();
    
    while (state !== "SUCCEEDED" && state !== "FAILED" && Date.now() - start < 10000) {
        await new Promise(r => setTimeout(r, 200));
        const poll = await fetch(`${host}/api/2.0/sql/statements/${statementId}`, {
            headers: { Authorization: `Bearer ${token}` }
        });
        const json = await poll.json();
        state = json.status.state;
        if (state === "SUCCEEDED") result = json.result;
    }

    if (!result) throw new Error("Query timed out");

    // Format for Dropdown: { label: "01", value: "01" }
    const options = result.data_array.map((row: string[]) => ({
        label: row[0],
        value: row[0]
    }));

    return res.status(200).json({ ok: true, options });

  } catch (err: any) {
    console.error("Filter API Error:", err);
    return res.status(500).json({ ok: false, error: err.message });
  }
}