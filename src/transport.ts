export interface Binding {
  type: "TEXT" | "FIXED" | "REAL" | "BOOLEAN";
  value: string;
}

export interface TransportOptions {
  endpoint: string;
  token: () => string | Promise<string>;
  tokenType?: "OAUTH" | "KEYPAIR_JWT";
  database: string;
  schema: string;
  warehouse?: string;
  role?: string;
  fetch?: typeof globalThis.fetch;
  timeoutMs?: number;
  pollIntervalMs?: number;
  maxPolls?: number;
}

export class SnowflakeError extends Error {
  public constructor(message: string, public readonly status?: number) {
    super(message);
    this.name = "SnowflakeError";
  }
}

function object(value: unknown): Record<string, unknown> {
  if (value === null || typeof value !== "object" || Array.isArray(value)) {
    throw new SnowflakeError("Invalid Snowflake response object");
  }
  return value as Record<string, unknown>;
}

function rows(value: unknown, width: number): (string | null)[][] {
  if (!Array.isArray(value) || !value.every(row => Array.isArray(row)
    && row.length === width && row.every(cell => cell === null || typeof cell === "string"))) {
    throw new SnowflakeError("Invalid Snowflake result rows");
  }
  return value as (string | null)[][];
}

function handle(value: unknown): string {
  if (typeof value !== "string" || !/^[\da-f]{8}-[\da-f]{4}-[\da-f]{4}-[\da-f]{4}-[\da-f]{12}$/i.test(value)) {
    throw new SnowflakeError("Invalid Snowflake statement handle");
  }
  return value;
}

export function integer(value: number, min: number, name: string): number {
  if (!Number.isSafeInteger(value) || value < min) throw new TypeError(`Invalid ${name}`);
  return value;
}

export class Transport {
  readonly #options: TransportOptions;
  readonly #endpoint: string;
  readonly #timeout: number;
  readonly #interval: number;
  readonly #polls: number;

  public constructor(options: TransportOptions) {
    const url = new URL(options.endpoint);
    if (url.protocol !== "https:" || !url.hostname.endsWith(".snowflakecomputing.com")
      || url.username || url.password || url.port || url.pathname !== "/" || url.search || url.hash) {
      throw new TypeError("Expected a Snowflake HTTPS account origin");
    }
    this.#endpoint = url.origin;
    this.#options = { ...options };
    this.#timeout = integer(options.timeoutMs ?? 60_000, 1, "timeoutMs");
    if (this.#timeout > 2_147_483_647) throw new TypeError("timeoutMs exceeds timer range");
    this.#interval = integer(options.pollIntervalMs ?? 250, 0, "pollIntervalMs");
    if (this.#interval > this.#timeout) throw new TypeError("pollIntervalMs exceeds timeoutMs");
    this.#polls = integer(options.maxPolls ?? 240, 1, "maxPolls");
    if (options.tokenType !== undefined && !["OAUTH", "KEYPAIR_JWT"].includes(options.tokenType)) {
      throw new TypeError("Unsupported token type");
    }
  }

  public async read(statement: string, bindings: Record<string, Binding>, maxRows: number): Promise<Record<string, string | null>[]> {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), this.#timeout);
    const signal = controller.signal;
    // Bound token acquisition, fetch, JSON decoding, and delays by one operation deadline.
    const bounded = async <T>(operation: Promise<T>): Promise<T> => {
      let onAbort: (() => void) | undefined;
      const aborted = new Promise<never>((_, reject) => {
        onAbort = () => reject(new SnowflakeError("Snowflake operation timed out"));
        if (signal.aborted) onAbort();
        else signal.addEventListener("abort", onAbort, { once: true });
      });
      try { return await Promise.race([operation, aborted]); }
      finally { if (onAbort) signal.removeEventListener("abort", onAbort); }
    };
    const request = async (path: string, body?: unknown) => {
      let token: string;
      try { token = await bounded(Promise.resolve().then(() => this.#options.token())); }
      catch { throw new SnowflakeError(signal.aborted ? "Snowflake operation timed out" : "Snowflake token provider failed"); }
      if (typeof token !== "string" || !token || /\s/.test(token)) throw new SnowflakeError("Invalid Snowflake token");
      let response: Response;
      try {
        response = await bounded((this.#options.fetch ?? globalThis.fetch)(this.#endpoint + path, {
          method: body === undefined ? "GET" : "POST", redirect: "error", signal,
          headers: { Authorization: `Bearer ${token}`, "X-Snowflake-Authorization-Token-Type": this.#options.tokenType ?? "OAUTH",
            "Content-Type": "application/json", Accept: "application/json", "User-Agent": "dalgo2snowflake/0.1.0" },
          ...(body === undefined ? {} : { body: JSON.stringify(body) }),
        }));
      } catch { throw new SnowflakeError(signal.aborted ? "Snowflake operation timed out" : "Snowflake request failed"); }
      if (![200, 202, 429].includes(response.status) || (body !== undefined && response.status === 429)) {
        throw new SnowflakeError("Snowflake request rejected", response.status);
      }
      let payload: Record<string, unknown>;
      try { payload = object(await bounded(response.json())); }
      catch { throw new SnowflakeError(signal.aborted ? "Snowflake operation timed out" : "Invalid Snowflake JSON response"); }
      return { status: response.status, payload };
    };
    try {
      let result = await request("/api/v2/statements", {
        statement, bindings, database: this.#options.database, schema: this.#options.schema,
        ...(this.#options.warehouse === undefined ? {} : { warehouse: this.#options.warehouse }),
        ...(this.#options.role === undefined ? {} : { role: this.#options.role }),
        timeout: Math.max(1, Math.ceil(this.#timeout / 1000)),
      });
      let polls = 0;
      const initialHandle = result.status === 200 ? undefined : handle(result.payload.statementHandle);
      while (result.status !== 200) {
        if (++polls > this.#polls) throw new SnowflakeError("Snowflake polling limit exceeded");
        let delay: ReturnType<typeof setTimeout> | undefined;
        try { await bounded(new Promise(resolve => { delay = setTimeout(resolve, this.#interval); })); }
        finally { clearTimeout(delay); }
        result = await request(`/api/v2/statements/${initialHandle}`);
      }
      const metadata = object(result.payload.resultSetMetaData);
      if (!Array.isArray(metadata.rowType)) throw new SnowflakeError("Missing Snowflake column metadata");
      const columns = metadata.rowType.map(column => {
        const name = object(column).name;
        if (typeof name !== "string" || name.length === 0) throw new SnowflakeError("Invalid Snowflake column metadata");
        return name;
      });
      if (new Set(columns).size !== columns.length) throw new SnowflakeError("Duplicate Snowflake column names");
      const count = metadata.numRows;
      if (typeof count !== "number" || !Number.isSafeInteger(count) || count < 0) throw new SnowflakeError("Invalid Snowflake row count");
      if (count > maxRows) throw new SnowflakeError("Snowflake result exceeds maxRows; use an explicit query limit");
      const partitions = metadata.partitionInfo;
      if (!Array.isArray(partitions) || partitions.length === 0) throw new SnowflakeError("Missing Snowflake partitions");
      const partitionCounts = partitions.map(partition => {
        const count = object(partition).rowCount;
        if (typeof count !== "number" || !Number.isSafeInteger(count) || count < 0) throw new SnowflakeError("Invalid Snowflake partition row count");
        return count;
      });
      if (partitionCounts.reduce((sum, count) => sum + count, 0) !== count
        || partitions.length > Math.max(1, count)) throw new SnowflakeError("Inconsistent Snowflake partition metadata");
      const allRows: (string | null)[][] = [];
      for (let i = 0; i < partitions.length; i++) {
        let payload = result.payload;
        if (i > 0) {
          const part = await request(`/api/v2/statements/${handle(result.payload.statementHandle)}?partition=${i}`);
          if (part.status !== 200) throw new SnowflakeError("Snowflake partition unavailable", part.status);
          payload = part.payload;
        }
        const partRows = rows(payload.data, columns.length);
        if (partRows.length !== object(partitions[i]).rowCount) throw new SnowflakeError("Incomplete Snowflake partition");
        allRows.push(...partRows);
        if (allRows.length > maxRows) throw new SnowflakeError("Snowflake result exceeds maxRows");
      }
      if (allRows.length !== count) throw new SnowflakeError("Incomplete Snowflake result");
      return allRows.map(row => Object.fromEntries(columns.map((name, i) => [name, row[i]!])));
    } finally { clearTimeout(timer); }
  }
}
