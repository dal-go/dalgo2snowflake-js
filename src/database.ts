import { DOCUMENT_ID, Key, UnsupportedError, identityCodec, type Codec, type Database,
  type QueryPage, type ReadwriteTransaction, type RecordSnapshot, type StructuredQuery } from "@dal-go/dalgo";
import { Transport, SnowflakeError, integer, type Binding, type TransportOptions } from "./transport.js";

export interface SnowflakeTable {
  /** Exact, case-sensitive SQL table name within the configured database/schema. */
  readonly table: string;
  /** Unique, non-null column. Snowflake standard tables do not enforce uniqueness. */
  readonly idColumn: string;
  readonly idType?: "string" | "number";
}

export interface SnowflakeOptions extends TransportOptions {
  readonly tables: Readonly<Record<string, SnowflakeTable>>;
  readonly maxRows?: number;
}

function identifier(value: string): string {
  if (typeof value !== "string" || value.length === 0 || value.includes("\0")) throw new TypeError("Invalid SQL identifier");
  return `"${value.replaceAll('"', '""')}"`;
}

function binding(value: unknown): Binding {
  if (typeof value === "string") return { type: "TEXT", value };
  if (typeof value === "boolean") return { type: "BOOLEAN", value: String(value) };
  if (typeof value === "number" && Number.isFinite(value)) {
    if (Number.isInteger(value) && !Number.isSafeInteger(value)) throw new TypeError("Unsafe integer query value; use a string");
    return { type: Number.isInteger(value) ? "FIXED" : "REAL", value: String(value) };
  }
  throw new UnsupportedError("Snowflake non-scalar query values");
}

export class SnowflakeDatabase implements Database {
  readonly #transport: Transport;
  readonly #tables: ReadonlyMap<string, SnowflakeTable>;
  readonly #maxRows: number;

  public constructor(options: SnowflakeOptions) {
    identifier(options.database); identifier(options.schema);
    if (options.warehouse !== undefined) identifier(options.warehouse);
    if (options.role !== undefined) identifier(options.role);
    this.#transport = new Transport(options);
    this.#maxRows = integer(options.maxRows ?? 10_000, 2, "maxRows");
    if (this.#maxRows >= Number.MAX_SAFE_INTEGER) throw new TypeError("maxRows is too large");
    this.#tables = new Map(Object.entries(options.tables).map(([name, table]) => {
      identifier(table.table); identifier(table.idColumn);
      if (table.idType !== undefined && table.idType !== "string" && table.idType !== "number") throw new TypeError("Invalid idType");
      return [name, { ...table }];
    }));
  }

  #table(name: string, parent?: Key): SnowflakeTable {
    if (parent !== undefined) throw new UnsupportedError("Snowflake parent keys");
    const table = this.#tables.get(name);
    if (!table) throw new UnsupportedError("Snowflake unmapped collection");
    return table;
  }

  #id(table: SnowflakeTable, id: unknown): string | number {
    if ((table.idType ?? "string") === "number") {
      if (typeof id !== "number" || !Number.isSafeInteger(id)) throw new TypeError("Expected a safe integer key");
    } else if (typeof id !== "string" || !id.length) throw new TypeError("Expected a non-empty string key");
    return id as string | number;
  }

  public async get<T>(key: Key, codec?: Codec<T>): Promise<RecordSnapshot<T>> {
    const table = this.#table(key.collection, key.parent);
    const id = this.#id(table, key.id);
    const rows = await this.#transport.read(`SELECT * FROM ${identifier(table.table)} WHERE ${identifier(table.idColumn)} = ? LIMIT 2`, { "1": binding(id) }, 2);
    if (rows.length > 1) throw new SnowflakeError("Snowflake key is not unique");
    const row = rows[0];
    if (!row) return { key, exists: false };
    if (typeof row[table.idColumn] !== "string" || row[table.idColumn] !== String(id)) throw new SnowflakeError("Snowflake returned a mismatched key");
    return { key, exists: true, data: (codec ?? identityCodec).decode(row) as T };
  }

  public async getMany<T>(keys: readonly Key[], codec?: Codec<T>): Promise<readonly RecordSnapshot<T>[]> {
    // Validate the entire batch before making requests; preserve input order and duplicates.
    for (const key of keys) this.#id(this.#table(key.collection, key.parent), key.id);
    const result: RecordSnapshot<T>[] = [];
    for (const key of keys) result.push(await this.get(key, codec));
    return result;
  }

  public async query<T>(query: StructuredQuery<T>): Promise<QueryPage<T>> {
    if (query.source.kind !== "collection") throw new UnsupportedError("Snowflake collection groups");
    const table = this.#table(query.source.name, query.source.parent);
    if ([query.startAt, query.startAfter, query.endAt, query.endBefore].some(value => value !== undefined)) {
      throw new UnsupportedError("Snowflake query cursors");
    }
    const bindings: Record<string, Binding> = {};
    const bind = (value: unknown) => { bindings[String(Object.keys(bindings).length + 1)] = binding(value); return "?"; };
    const column = (field: string) => {
      if (field.includes(".")) throw new UnsupportedError("Snowflake nested field paths");
      return identifier(field === DOCUMENT_ID ? table.idColumn : field);
    };
    const idValue = (value: unknown): unknown => {
      if (value instanceof Key) {
        if (value.collection !== query.source.name || value.parent) throw new TypeError("Document key belongs to another collection");
        value = value.id;
      }
      return this.#id(table, value);
    };
    const filters = query.filters.map(filter => {
      const field = column(filter.field);
      let value = filter.value;
      if (filter.operator === "in" || filter.operator === "not-in") {
        if (!Array.isArray(value) || value.length === 0 || value.some(item => item === null)) throw new UnsupportedError("Snowflake empty or null-containing membership filters");
        return `${field} ${filter.operator === "in" ? "IN" : "NOT IN"} (${value.map(item => bind(filter.field === DOCUMENT_ID ? idValue(item) : item)).join(", ")})`;
      }
      if (filter.field === DOCUMENT_ID) value = idValue(value);
      if (value === null) {
        if (filter.operator === "==") return `${field} IS NULL`;
        if (filter.operator === "!=") return `${field} IS NOT NULL`;
        throw new UnsupportedError("Snowflake ordered null comparisons");
      }
      const operators: Record<string, string> = { "==": "=", "!=": "<>", "<": "<", "<=": "<=", ">": ">", ">=": ">=" };
      if (!Object.hasOwn(operators, filter.operator)) throw new UnsupportedError("Snowflake query operator");
      return `${field} ${operators[filter.operator]} ${bind(value)}`;
    });
    const orders = query.orders.map(order => {
      if (order.direction !== "asc" && order.direction !== "desc") throw new TypeError("Invalid query order direction");
      return `${column(order.field)} ${order.direction.toUpperCase()} NULLS LAST`;
    });
    if (!query.orders.some(order => order.field === DOCUMENT_ID || order.field === table.idColumn)) orders.push(`${identifier(table.idColumn)} ASC`);
    const limit = query.limit === undefined ? this.#maxRows + 1 : integer(query.limit, 1, "limit");
    if (query.limit !== undefined && limit > this.#maxRows) throw new RangeError("limit exceeds maxRows");
    const offset = integer(query.offset ?? 0, 0, "offset");
    const sql = `SELECT * FROM ${identifier(table.table)}${filters.length ? ` WHERE ${filters.join(" AND ")}` : ""} ORDER BY ${orders.join(", ")} LIMIT ${limit} OFFSET ${offset}`;
    const rows = await this.#transport.read(sql, bindings, this.#maxRows);
    const ids = new Set<string | number>();
    const records = rows.map(row => {
      const rawId = row[table.idColumn];
      if (rawId === null || rawId === undefined || rawId === "") throw new SnowflakeError("Snowflake row lacks a key");
      let id: string | number = rawId;
      if (table.idType === "number") {
        if (!/^-?\d+$/.test(rawId)) throw new SnowflakeError("Snowflake numeric key is not an integer");
        id = this.#id(table, Number(rawId));
      }
      if (ids.has(id)) throw new SnowflakeError("Snowflake key is not unique");
      ids.add(id);
      return { key: new Key(query.source.name, id), exists: true as const, data: (query.source.codec ?? identityCodec).decode(row) as T };
    });
    return { records };
  }

  public runReadwriteTransaction<Result>(callback: (transaction: ReadwriteTransaction) => Promise<Result>): Promise<Result> {
    void callback;
    return Promise.reject(new UnsupportedError("Snowflake interactive read-write transactions"));
  }
}
