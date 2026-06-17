import { QueryResult } from "./types.js";

export class LightningClient {
  private baseUrl: string;

  constructor(baseUrl: string) {
    this.baseUrl = baseUrl.replace(/\/+$/, "");
  }

  async health(): Promise<number> {
    try {
      const res = await fetch(`${this.baseUrl}/health`, {
        method: "GET",
        signal: AbortSignal.timeout(3000),
      });
      return res.status;
    } catch {
      return 0;
    }
  }

  async query(
    statement: string,
    params?: Record<string, unknown>
  ): Promise<QueryResult> {
    const body: Record<string, unknown> = { query: statement };
    if (params) body.params = params;

    const res = await fetch(`${this.baseUrl}/v1/query`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
      signal: AbortSignal.timeout(15000),
    });

    if (!res.ok) {
      const text = await res.text();
      let msg: string;
      try {
        const err = JSON.parse(text);
        msg = err.error || err.message || text;
      } catch {
        msg = text;
      }
      throw new Error(`HTTP ${res.status}: ${msg}`);
    }

    const json = await res.json() as { data: QueryResult };
    return (json.data ?? json) as QueryResult;
  }

  async queryRaw(statement: string): Promise<{
    status: number;
    body: string;
  }> {
    const res = await fetch(`${this.baseUrl}/v1/query`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ query: statement }),
      signal: AbortSignal.timeout(15000),
    });
    return { status: res.status, body: await res.text() };
  }
}
