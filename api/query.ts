import type { VercelRequest, VercelResponse } from "@vercel/node";

export default async function handler(
  req: VercelRequest,
  res: VercelResponse
) {
  // ✅ ALWAYS set CORS header on the response
  res.setHeader(
    "Access-Control-Allow-Origin",
    "https://brickhouser3.github.io"
  );
  res.setHeader("Vary", "Origin");

  if (req.method === "OPTIONS") {
    return res.status(200).end();
  }

  if (req.method !== "POST") {
    return res.status(405).json({ error: "Method not allowed" });
  }

  try {
    const body =
      typeof req.body === "string"
        ? JSON.parse(req.body)
        : req.body;

    const response = await fetch(
      `${process.env.DATABRICKS_HOST}/api/2.0/sql/statements`,
      {
        method: "POST",
        headers: {
          Authorization: `Bearer ${process.env.DATABRICKS_TOKEN}`,
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          statement:
            "select max(cal_dt) as max_cal_dt from vip.bir.bir_weekly_ind",
          warehouse_id: process.env.WAREHOUSE_ID,
        }),
      }
    );

    if (!response.ok) {
      const text = await response.text();
      throw new Error(text);
    }

    const data = await response.json();

    // ✅ CORS header will now be present on 200 response
    return res.status(200).json(data);
  } catch (err: any) {
    console.error("❌ Databricks query failed:", err);

    return res.status(500).json({
      error: "Databricks query failed",
      details: err.message,
    });
  }
}
