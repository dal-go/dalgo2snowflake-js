# DALGO Snowflake adapter

Read-only [DALGO TypeScript](https://github.com/dal-go/dalgo-js) `Database` adapter for the Snowflake SQL API. Classification: **HTTP-capable, not browser-ready**. Intended for a trusted server runtime with native Fetch (Node.js 22+). No Snowflake SDK or arbitrary SQL execution interface is exposed.

## Install and use

The package is source-distributed; an npm release is not claimed. Pin Git dependencies to reviewed commits for reproducible installations. Development uses DALGO commit `04ce7f644fc334da7e471f0be503a7b937c7025d`.

```sh
pnpm add github:dal-go/dalgo-js#04ce7f644fc334da7e471f0be503a7b937c7025d github:dal-go/dalgo2snowflake-js
```

```ts
import { collection } from "@dal-go/dalgo";
import { SnowflakeDatabase } from "@dal-go/dalgo2snowflake";

const db = new SnowflakeDatabase({
  endpoint: "https://my-org-my-account.snowflakecomputing.com",
  database: "APP", schema: "PUBLIC", warehouse: "COMPUTE_WH",
  token: async () => obtainOAuthAccessToken(), // your server-side token provider
  tables: { users: { table: "USERS", idColumn: "ID" } },
});
const users = collection<Record<string, string | null>>("users");
const user = await db.get(users.key("user-1"));
const page = await db.query(users.query().where("NAME", "==", "Ada").limit(20).build());
```

See [the typechecked codec example](examples/read.ts). `get` accepts a codec separately; a collection's codec applies automatically to its queries. Tables must already exist. Identifier spelling is exact and case-sensitive; ordinary unquoted Snowflake identifiers are normally uppercase. Collection names map only through `tables`, within one configured database/schema. `idColumn` must hold unique, non-null values. Default keys are non-empty strings; `idType: "number"` permits safe integers. Standard Snowflake tables do not enforce primary-key uniqueness: maintain this invariant in ingestion. Duplicate keys encountered in a result cause failure; uniqueness outside a limited result is not audited.

## Capabilities and semantics

- `get` returns DALGO existing/missing snapshots. `getMany` preserves input order, missing records and duplicates, using sequential point reads; it is not a shared snapshot.
- Collection queries support scalar `==`, `!=`, `<`, `<=`, `>`, `>=`, `in`, `not-in`; filters are ANDed. `== null` and `!= null` become SQL null predicates. Other comparisons follow SQL semantics, including exclusion of null rows from ordinary inequality and membership comparisons. Null-containing or empty membership lists are rejected.
- Query fields are exact top-level SQL columns. `DOCUMENT_ID` addresses `idColumn`, accepting an ID or matching DALGO `Key`. Ordering specifies `NULLS LAST`; an ascending ID tie-breaker is appended unless the ID is already ordered. Limits and offsets are supported. Offsets across separate queries can shift as underlying data changes.
- Returned row values retain Snowflake JSONv2 strings and nulls, including numbers, booleans, temporal values and JSON text. Use a codec to decode domain types without silently losing numeric precision. The ID remains present in record data.
- Async execution is polled; all result partitions are fetched. `maxRows` defaults to 10,000 (minimum 2). Queries without an explicit limit request one extra row to detect overflow and throw, never silently truncate. Explicit limits above `maxRows` are rejected. Returned pages have no `nextCursor`.

`runReadwriteTransaction` rejects with DALGO `UnsupportedError` **without invoking the callback**. DALGO interactive callback transactions require read-dependent writes within one transaction; the SQL API's documented multi-statement transaction facility does not provide that callback contract. Insert/set/update/delete are not exposed. Collection groups, parent keys, cursor boundaries, nested field paths, array operators, object/date/bigint filter values are also unsupported and rejected explicitly.

## Credentials and transport

Use a least-privilege read-only Snowflake role. `token` is called for each request, supporting rotation. OAuth is the default; pre-generated key-pair JWTs can use `tokenType: "KEYPAIR_JWT"`. Token generation, refresh, storage and signing remain the host application's responsibility. Never embed credentials in a browser bundle.

The endpoint must be an HTTPS `*.snowflakecomputing.com` account origin with no credentials, port, query or path. Redirects are refused. Server-provided status URLs are ignored; validated statement handles generate same-origin paths. Tokens are sent only in headers. Provider/fetch/server error text is deliberately omitted from adapter errors to avoid credential, SQL or data disclosure. A supplied `fetch` implementation is trusted and must implement standard Fetch security/decompression behavior.

`timeoutMs` (default 60,000) bounds the complete operation, including token acquisition and body decoding. `maxPolls` (240) and `pollIntervalMs` (250) bound pending execution polling; GET 429 is treated as pending. POST failures are not retried. A timeout or polling limit stops local waiting; it does not cancel the remote statement. The statement also receives a server execution timeout. JSON responses are buffered in memory; the row cap is not a byte cap.

Browser CORS, OAuth browser flows and Snowflake account policy have not been certified. Tests use in-process contract fetch mocks; **no live Snowflake account or external browser journey has been tested**.

## Development

```sh
pnpm install --frozen-lockfile
pnpm check
```

Tooling: pnpm 11.20, TypeScript 6, ESLint and Vitest. CI typechecks the adapter, tests and example, runs tests, and builds ESM/declarations. GitHub default CodeQL is used; no custom CodeQL workflow is included. Packaging follows the [Firestore adapter](https://github.com/dal-go/dalgo2firestore-js).

Wire references: Snowflake [bindings and submission](https://docs.snowflake.com/en/developer-guide/sql-api/submitting-requests), [polling, partitions and value encoding](https://docs.snowflake.com/en/developer-guide/sql-api/handling-responses), [authentication](https://docs.snowflake.com/en/developer-guide/sql-api/authenticating), [transactions](https://docs.snowflake.com/en/developer-guide/sql-api/using-transactions).
