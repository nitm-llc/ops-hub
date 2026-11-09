// A D1-shaped wrapper over node:sqlite (in-memory), so tests run real SQL:
// prepare().bind().run()/all()/first(), batch(). Not a full D1 emulation.
import { DatabaseSync } from "node:sqlite";

export function sqliteD1() {
  const db = new DatabaseSync(":memory:");
  const clean = (args) => args.map((a) => (a === undefined ? null : typeof a === "boolean" ? (a ? 1 : 0) : a));
  const stmt = (sql, args = []) => ({
    bind: (...a) => stmt(sql, clean(a)),
    async run() {
      const r = db.prepare(sql).run(...args);
      return { success: true, meta: { changes: Number(r.changes), last_row_id: Number(r.lastInsertRowid) } };
    },
    async all() { return { results: db.prepare(sql).all(...args).map((r) => ({ ...r })) }; },
    async first(col) {
      const r = db.prepare(sql).get(...args);
      if (!r) return null;
      return col ? r[col] : { ...r };
    },
  });
  return {
    prepare: (sql) => stmt(sql),
    async batch(list) {
      db.exec("BEGIN");
      try { const out = []; for (const s of list) out.push(await s.run()); db.exec("COMMIT"); return out; }
      catch (e) { db.exec("ROLLBACK"); throw e; }
    },
    raw: db,
  };
}
