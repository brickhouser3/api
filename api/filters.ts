import type { VercelRequest, VercelResponse } from "@vercel/node";

// ... (Keep your imports and setCors function exactly as they were) ...

function setCors(req: VercelRequest, res: VercelResponse) {
  const origin = req.headers.origin || "";
  res.setHeader("Access-Control-Allow-Origin", origin);
  res.setHeader("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type, Authorization, x-mc-api, x-mc-version");
}

export default async function handler(req: VercelRequest, res: VercelResponse) {
  setCors(req, res);
  if (req.method === "OPTIONS") return res.status(200).end();

  if (req.method !== "POST") {
      return res.status(405).json({ ok: false, error: "Method not allowed." });
  }

  try {
    const rawBody = req.body;
    const body = rawBody ? (typeof rawBody === "string" ? JSON.parse(rawBody) : rawBody) : {};

    const { dimension, table, months } = body;

    if (!dimension || !table) {
        return res.status(400).json({ ok: false, error: "Missing dimension or table" });
    }

    const ALLOWED_COLS = ["wslr_nbr", "mktng_st_cd", "sls_regn_cd", "channel", "megabrand"];
    const ALLOWED_TABLES = ["mbmc_actuals_volume", "mbmc_actuals_revenue", "mbmc_actuals_distro"];

    if (!ALLOWED_COLS.includes(dimension) || !ALLOWED_TABLES.includes(table)) {
        return res.status(400).json({ ok: false, error: "Invalid parameters" });
    }

    // ✅ NEW: Handle Month Filtering Logic
    let dateFilter = "";
    if (months && Array.isArray(months) && months.length > 0) {
        // Safe sanitization: ensure strings look like numbers/dates
        const safeMonths = months.filter(m => /^\d+$/.test(m));
        if (safeMonths.length > 0) {
            const list = safeMonths.map(m => `'${m}'`).join(",");
            dateFilter = `AND month IN (${list})`;
        }
    }

    const host = process.env.DATABRICKS_HOST;
    const token = process.env.DATABRICKS_TOKEN;
    const warehouseId = process.env.WAREHOUSE_ID;

    // ✅ UPDATED SQL: Includes the date filter
    const sql = `
      SELECT DISTINCT ${dimension} as label 
      FROM commercial_dev.capabilities.${table} 
      WHERE ${dimension} IS NOT NULL 
      ${dateFilter} 
      ORDER BY label ASC 
      LIMIT 2000
    `;

    const submitRes = await fetch(`${host}/api/2.0/sql/statements`, {
      method: "POST",
      headers: { Authorization: `Bearer ${token}`, "Content-Type": "application/json" },
      body: JSON.stringify({ statement: sql, warehouse_id: warehouseId, wait_timeout: "30s" }),
    });

    const submitted = await submitRes.json();
    const statementId = submitted?.statement_id;

    // ... (Keep existing polling logic unchanged) ...
    // Note: If you have the polling loop from previous steps, keep it here.
    // Minimally, ensure you wait for result:
    
    // Quick Poll Mockup (Replace with your full polling loop)
    let result = null;
    if (submitted.status?.state === "SUCCEEDED") {
        result = submitted.result;
    } else {
        // Simple wait (Production should use the full loop provided previously)
        await new Promise(r => setTimeout(r, 2000));
        const poll = await fetch(`${host}/api/2.0/sql/statements/${statementId}`, { headers: { Authorization: `Bearer ${token}` }});
        const json = await poll.json();
        result = json.result;
    }

    if (!result) throw new Error("Query pending... refresh to retry");

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