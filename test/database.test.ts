import { describe, expect, it, vi } from "vitest";
import { collection, collectionGroup, DOCUMENT_ID, Key, UnsupportedError, type Codec } from "@dal-go/dalgo";
import { SnowflakeDatabase, SnowflakeError, type SnowflakeOptions } from "../src/index.js";

const statementHandle = "536fad38-b564-4dc5-9892-a4543504df6c";
const endpoint = "https://example-account.snowflakecomputing.com";
const users = collection<Record<string, string | null>>("users");
function result(data: (string | null)[][] = [], counts = [data.length]) {
  return { statementHandle, resultSetMetaData: {
    numRows: counts.reduce((sum, count) => sum + count, 0),
    rowType: [{ name: "ID", type: "text" }, { name: "NAME", type: "text" }],
    partitionInfo: counts.map(rowCount => ({ rowCount })),
  }, data };
}
function fixture(responses: { body: unknown; status?: number }[], overrides: Partial<SnowflakeOptions> = {}) {
  const requests: { url: string; init: RequestInit }[] = [];
  const fetch = vi.fn<typeof globalThis.fetch>(async (url, init) => {
    requests.push({ url: String(url), init: init ?? {} });
    const response = responses.shift();
    if (!response) throw new Error("Unexpected HTTP request");
    return new Response(JSON.stringify(response.body), { status: response.status ?? 200 });
  });
  const token = vi.fn(() => "test-secret");
  const db = new SnowflakeDatabase({ endpoint, database: "DB", schema: "PUBLIC", tables: { users: { table: "USERS", idColumn: "ID" } }, token, fetch, pollIntervalMs: 0, ...overrides });
  return { db, requests, fetch, token };
}
function body(request: { init: RequestInit } | undefined): Record<string, unknown> {
  return JSON.parse(String(request?.init.body)) as Record<string, unknown>;
}

describe("DALGO reads", () => {
  it("binds point keys, preserves the requested key, returns missing snapshots", async () => {
    const { db, requests } = fixture([{ body: result([["a' OR true --", "Ada"]]) }, { body: result() }]);
    const key = users.key("a' OR true --");
    expect(await db.get(key)).toEqual({ key, exists: true, data: { ID: key.id, NAME: "Ada" } });
    expect(body(requests[0])).toMatchObject({ statement: 'SELECT * FROM "USERS" WHERE "ID" = ? LIMIT 2', bindings: { "1": { type: "TEXT", value: key.id } }, database: "DB", schema: "PUBLIC" });
    expect(await db.get(users.key("absent"))).toEqual({ key: users.key("absent"), exists: false });
  });

  it("decodes through a codec and preserves getMany ordering, duplicates and empty input", async () => {
    const { db, fetch } = fixture([{ body: result([["b", "Bee"]]) }, { body: result() }, { body: result([["b", "Bee"]]) }]);
    const codec: Codec<string> = { encode: value => value, decode: value => (value as Record<string, string>).NAME! };
    expect(await db.getMany([], codec)).toEqual([]);
    expect(fetch).not.toHaveBeenCalled();
    const records = await db.getMany([users.key("b"), users.key("a"), users.key("b")], codec);
    expect(records.map(record => record.exists ? record.data : null)).toEqual(["Bee", null, "Bee"]);
  });

  it("rejects duplicates, wrong keys and incomplete response rows", async () => {
    for (const payload of [result([["a", "Ada"], ["a", "Another"]]), result([["b", "Wrong"]]), result([["a", "Ada"]], [2])]) {
      const { db } = fixture([{ body: payload }]);
      await expect(db.get(users.key("a"))).rejects.toBeInstanceOf(SnowflakeError);
    }
  });

  it("validates all getMany keys before network access", async () => {
    const { db, fetch } = fixture([]);
    await expect(db.getMany([users.key("a"), new Key("users", "b", users.key("a"))])).rejects.toBeInstanceOf(UnsupportedError);
    expect(fetch).not.toHaveBeenCalled();
  });
});

describe("structured SQL compilation", () => {
  it("compiles scalar, null, membership, order, limit and offset with positional bindings", async () => {
    const { db, requests } = fixture([{ body: result([["a", null]]) }]);
    const page = await db.query(users.query().where("NAME", "!=", "Robert'); DROP TABLE USERS;--")
      .where("AGE", ">=", 21).where("ACTIVE", "==", true).where("SCORE", "<", 1.5)
      .where("MISSING", "==", null).where("NAME", "in", ["Ada", "Lin"])
      .orderBy("NAME", "desc").limit(3).offset(4).build());
    expect(body(requests[0])).toMatchObject({
      statement: 'SELECT * FROM "USERS" WHERE "NAME" <> ? AND "AGE" >= ? AND "ACTIVE" = ? AND "SCORE" < ? AND "MISSING" IS NULL AND "NAME" IN (?, ?) ORDER BY "NAME" DESC NULLS LAST, "ID" ASC LIMIT 3 OFFSET 4',
      bindings: { "1": { type: "TEXT", value: "Robert'); DROP TABLE USERS;--" }, "2": { type: "FIXED", value: "21" }, "3": { type: "BOOLEAN", value: "true" }, "4": { type: "REAL", value: "1.5" }, "5": { type: "TEXT", value: "Ada" }, "6": { type: "TEXT", value: "Lin" } },
    });
    expect(page).toEqual({ records: [{ key: users.key("a"), exists: true, data: { ID: "a", NAME: null } }] });
  });

  it("quotes hostile identifiers and resolves DOCUMENT_ID Key values", async () => {
    const { db, requests } = fixture([{ body: result() }], { tables: { users: { table: 'T"; DROP TABLE T;--', idColumn: "ID" } } });
    await db.query(users.query().where(DOCUMENT_ID, "in", [users.key("a"), "b"]).orderBy('x" DESC;--').build());
    expect(body(requests[0]).statement).toBe('SELECT * FROM "T""; DROP TABLE T;--" WHERE "ID" IN (?, ?) ORDER BY "x"" DESC;--" ASC NULLS LAST, "ID" ASC LIMIT 10001 OFFSET 0');
    expect(body(requests[0]).bindings).toEqual({ "1": { type: "TEXT", value: "a" }, "2": { type: "TEXT", value: "b" } });
  });

  it("supports numeric keys without lossy conversion", async () => {
    const { db, requests } = fixture([{ body: result([["42", "Ada"]]) }, { body: result([["42", "Ada"]]) }, { body: result([["9007199254740993", "Big"]]) }], { tables: { users: { table: "USERS", idColumn: "ID", idType: "number" } } });
    expect((await db.get(new Key("users", 42))).exists).toBe(true);
    expect(body(requests[0]).bindings).toEqual({ "1": { type: "FIXED", value: "42" } });
    expect((await db.query(users.query().build())).records[0]?.key.id).toBe(42);
    await expect(db.query(users.query().build())).rejects.toThrow("safe integer");
  });

  it("rejects unsupported semantics before HTTP", async () => {
    const { db, fetch } = fixture([]);
    for (const query of [collectionGroup("users").build(), users.in(users.key("p")).query().build(), users.query().startAfter("a").build(), users.query().where("tags", "array-contains", "a").build(), users.query().where("nested.field", "==", "x").build(), users.query().where("NAME", "in", []).build(), users.query().where("NAME", "not-in", [null]).build(), users.query().where("NAME", "==", {}).build(), users.query().where(DOCUMENT_ID, "==", new Key("other", "a")).build(), users.query().where("N", "==", Number.NaN).build(), { ...users.query().build(), limit: -1 }]) {
      await expect(db.query(query)).rejects.toThrow();
    }
    const callback = vi.fn();
    await expect(db.runReadwriteTransaction(callback)).rejects.toBeInstanceOf(UnsupportedError);
    expect(callback).not.toHaveBeenCalled();
    expect(fetch).not.toHaveBeenCalled();
  });

  it("fails oversized results instead of silently truncating and rejects null/duplicate query keys", async () => {
    for (const payload of [result([["a", "A"], ["b", "B"], ["c", "C"]]), result([[null, "A"]]), result([["a", "A"], ["a", "B"]])]) {
      const { db } = fixture([{ body: payload }], { maxRows: 2 });
      await expect(db.query(users.query().build())).rejects.toBeInstanceOf(SnowflakeError);
    }
  });
});

describe("Snowflake SQL API HTTP contract", () => {
  it("polls 202 and GET 429, fetches partitions, rotates tokens, and ignores server URLs", async () => {
    const { db, requests, token } = fixture([
      { status: 202, body: { statementHandle, statementStatusUrl: "https://evil.example/steal" } },
      { status: 429, body: { statementHandle } },
      { body: result([["a", "Ada"]], [1, 1]) },
      { body: { data: [["b", "Ben"]] } },
    ]);
    expect((await db.query(users.query().build())).records.map(record => record.key.id)).toEqual(["a", "b"]);
    expect(requests.map(request => request.url)).toEqual([`${endpoint}/api/v2/statements`, `${endpoint}/api/v2/statements/${statementHandle}`, `${endpoint}/api/v2/statements/${statementHandle}`, `${endpoint}/api/v2/statements/${statementHandle}?partition=1`]);
    expect(token).toHaveBeenCalledTimes(4);
    for (const request of requests) {
      expect(request.init.redirect).toBe("error");
      expect(request.init.headers).toMatchObject({ Authorization: "Bearer test-secret", "X-Snowflake-Authorization-Token-Type": "OAUTH" });
      expect(request.url).not.toContain("test-secret");
    }
  });

  it.each([301, 401, 403, 422, 429, 500])("redacts server errors with status %i", async status => {
    const { db } = fixture([{ status, body: { message: "test-secret", statement: "sensitive sql" } }]);
    await expect(db.get(users.key("a"))).rejects.toMatchObject({ message: "Snowflake request rejected", status });
  });

  it("redacts token provider and fetch exceptions", async () => {
    for (const overrides of [{ token: () => { throw new Error("test-secret"); } }, { fetch: vi.fn<typeof globalThis.fetch>(() => Promise.reject(new Error("test-secret"))) }]) {
      const { db } = fixture([], overrides);
      await expect(db.get(users.key("a"))).rejects.not.toThrow("test-secret");
    }
  });

  it("bounds polling and rejects malicious handles", async () => {
    const { db } = fixture([{ status: 202, body: { statementHandle } }, { status: 202, body: { statementHandle } }], { maxPolls: 1 });
    await expect(db.get(users.key("a"))).rejects.toThrow("polling limit");
    const malformed = fixture([{ status: 202, body: { statementHandle: "../../evil" } }]);
    await expect(malformed.db.get(users.key("a"))).rejects.toThrow("statement handle");
    expect(malformed.fetch).toHaveBeenCalledTimes(1);
  });

  it("bounds a token provider that never resolves", async () => {
    const { db } = fixture([], { token: () => new Promise<string>(() => {}), timeoutMs: 5 });
    await expect(db.get(users.key("a"))).rejects.toThrow("timed out");
  });

  it("bounds fetch and response body decoding", async () => {
    for (const fetch of [
      vi.fn<typeof globalThis.fetch>(() => new Promise<Response>(() => {})),
      vi.fn<typeof globalThis.fetch>(async () => new Response(new ReadableStream())),
    ]) {
      const { db } = fixture([], { fetch, timeoutMs: 5 });
      await expect(db.get(users.key("a"))).rejects.toThrow("timed out");
    }
  });

  it("validates partition metadata before fetching additional partitions", async () => {
    const { db, fetch } = fixture([{ body: { ...result([["a", "Ada"]], [1, 1]), resultSetMetaData: { ...result().resultSetMetaData, numRows: 2, partitionInfo: [{ rowCount: 1 }, { rowCount: -1 }] } } }]);
    await expect(db.query(users.query().build())).rejects.toThrow("partition row count");
    expect(fetch).toHaveBeenCalledTimes(1);
  });

  it("rejects null identifiers even when the requested key is the string null", async () => {
    const { db } = fixture([{ body: result([[null, "Ada"]]) }]);
    await expect(db.get(users.key("null"))).rejects.toThrow("mismatched key");
  });

  it("rejects malformed metadata and data", async () => {
    for (const payload of [null, {}, { ...result(), resultSetMetaData: {} }, { ...result(), data: [[7, "Ada"]] }, { ...result(), resultSetMetaData: { ...result().resultSetMetaData, numRows: 1 } }]) {
      const { db } = fixture([{ body: payload }]);
      await expect(db.get(users.key("a"))).rejects.toBeInstanceOf(SnowflakeError);
    }
  });

  it.each(["http://example.snowflakecomputing.com", "https://evil.example", "https://a.snowflakecomputing.com.evil.example", "https://user:pass@example.snowflakecomputing.com", "https://example.snowflakecomputing.com/path", "https://example.snowflakecomputing.com?token=secret"])("rejects unsafe endpoint %s", endpoint => {
    expect(() => fixture([], { endpoint })).toThrow();
  });
});
