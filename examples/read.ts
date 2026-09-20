import { collection, type Codec } from "@dal-go/dalgo";
import { SnowflakeDatabase } from "../src/index.js";

export async function readUsers(endpoint: string, token: () => Promise<string>) {
  type User = { ID: string; NAME: string };
  const codec: Codec<User> = {
    encode: value => value,
    decode(value) {
      const row = value as Record<string, unknown>;
      if (typeof row.ID !== "string" || typeof row.NAME !== "string") throw new TypeError("Invalid user row");
      return { ID: row.ID, NAME: row.NAME };
    },
  };
  const db = new SnowflakeDatabase({ endpoint, token, database: "APP", schema: "PUBLIC", warehouse: "COMPUTE_WH", tables: { users: { table: "USERS", idColumn: "ID" } } });
  const users = collection<User>("users", { codec });
  return { user: await db.get(users.key("user-1"), codec), page: await db.query(users.query().where("NAME", "==", "Ada").orderBy("NAME").limit(20).build()) };
}
