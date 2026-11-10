// ============================================================================
// CX SCORECARD AUDIT — read-side queries for the dashboard (and the digest).
//
// Pass rate = pass / (pass + fail) over check results. "na" and "off" are not
// counted: a check that didn't apply is neither a pass nor a fail.
//
// A conversation/day/author can have rows under several guidelines versions
// (re-scored after a guidelines change). Reports use the NEWEST row for each,
// unless ?version= pins one, so nothing is double counted.
// ============================================================================

import { db } from "./store.js";
import { problemFor } from "./checks.js";

const DAY = /^\d{4}-\d{2}-\d{2}$/;

export function parseFilters(params) {
  const get = (k) => (params.get ? params.get(k) : params[k]) || "";
  const f = {
    from: get("from"), to: get("to"),
    author: ["ivy", "team"].includes(get("author")) ? get("author") : "",
    check: /^[a-z_]{2,40}$/.test(get("check")) ? get("check") : "",
    issue: String(get("issue")).slice(0, 80),
    version: /^v\d+$/.test(get("version")) ? get("version") : "",
  };
  if (!DAY.test(f.from) || !DAY.test(f.to) || f.from > f.to) throw new Error("from and to must be YYYY-MM-DD, from <= to");
  return f;
}

// Latest score row per conversation/day/author within the range, filtered.
// Returns [sql, binds] for a CTE named `s`.
function scoresCte(f) {
  const where = ["audit_day >= ?", "audit_day <= ?"];
  const binds = [f.from, f.to];
  if (f.author) { where.push("author_type = ?"); binds.push(f.author); }
  if (f.version) { where.push("guidelines_version = ?"); binds.push(f.version); }
  const outer = [];
  if (f.issue) { outer.push("COALESCE(issue_type, '(none)') = ?"); binds.push(f.issue); }
  const sql = `WITH ranked AS (
      SELECT *, ROW_NUMBER() OVER (PARTITION BY conversation_id, audit_day, author_type ORDER BY scored_at DESC, id DESC) AS rn
      FROM cxa_scores WHERE ${where.join(" AND ")}
    ), s AS (SELECT * FROM ranked WHERE rn = 1${outer.length ? " AND " + outer.join(" AND ") : ""})`;
  return [sql, binds];
}

const checkFilter = (f) => (f.check ? ["AND c.check_id = ?", [f.check]] : ["", []]);

export async function summary(env, f) {
  const d = db(env);
  const [cte, b] = scoresCte(f);
  const [cf, cb] = checkFilter(f);
  const all = async (sql, binds) => (await d.prepare(sql).bind(...binds).all()).results;

  const perCheck = await all(`${cte}
    SELECT c.check_id, s.author_type, SUM(c.result = 'pass') AS pass, SUM(c.result = 'fail') AS fail, SUM(c.result = 'na') AS na
    FROM s JOIN cxa_check_results c ON c.score_id = s.id
    GROUP BY c.check_id, s.author_type`, b);

  const byIssue = await all(`${cte}
    SELECT COALESCE(s.issue_type, '(none)') AS issue_type, s.author_type,
      SUM(c.result = 'pass') AS pass, SUM(c.result = 'fail') AS fail, COUNT(DISTINCT s.id) AS convs
    FROM s JOIN cxa_check_results c ON c.score_id = s.id WHERE 1 = 1 ${cf}
    GROUP BY 1, s.author_type`, [...b, ...cb]);

  const trend = await all(`${cte}
    SELECT s.audit_day AS day, s.author_type,
      SUM(c.result = 'pass') AS pass, SUM(c.result = 'fail') AS fail, COUNT(DISTINCT s.id) AS convs
    FROM s JOIN cxa_check_results c ON c.score_id = s.id WHERE 1 = 1 ${cf}
    GROUP BY s.audit_day, s.author_type ORDER BY s.audit_day`, [...b, ...cb]);

  const totals = await all(`${cte}
    SELECT s.author_type, COUNT(DISTINCT s.id) AS convs,
      SUM(c.result = 'pass') AS pass, SUM(c.result = 'fail') AS fail
    FROM s JOIN cxa_check_results c ON c.score_id = s.id WHERE 1 = 1 ${cf}
    GROUP BY s.author_type`, [...b, ...cb]);

  // Filter options come from the whole range, ignoring the other filters.
  const issueTypes = (await all(
    "SELECT DISTINCT COALESCE(issue_type, '(none)') AS t FROM cxa_scores WHERE audit_day >= ? AND audit_day <= ? ORDER BY t", [f.from, f.to]
  )).map((r) => r.t);
  const versions = (await all(
    "SELECT DISTINCT guidelines_version AS v FROM cxa_scores WHERE audit_day >= ? AND audit_day <= ? ORDER BY v", [f.from, f.to]
  )).map((r) => r.v);
  const finChanges = await all("SELECT id, day, what, who FROM cxa_fin_changes WHERE day >= ? AND day <= ? ORDER BY day", [f.from, f.to]);
  const lastRun = await d.prepare("SELECT id, kind, day_from, day_to, status, scored, finished_at FROM cxa_runs WHERE status = 'done' ORDER BY id DESC LIMIT 1").first();

  return {
    filters: f,
    totals: Object.fromEntries(totals.map((t) => [t.author_type, { convs: t.convs, pass: t.pass, fail: t.fail }])),
    per_check: perCheck, by_issue: byIssue, trend,
    issue_types: issueTypes, versions, fin_changes: finChanges, last_run: lastRun,
  };
}

// The worst-scoring replies: most failed checks first, then the lowest pass
// share. One row per conversation/day/author, with every failed check.
export async function worstReplies(env, f, n = 3) {
  const d = db(env);
  const [cte, b] = scoresCte(f);
  const { results: rows } = await d.prepare(`${cte}
    SELECT id, audit_day AS day, author_type, conversation_id, link, issue_type, failed, passed, applicable, overall_note
    FROM s WHERE failed > 0
    ORDER BY failed DESC, CAST(passed AS REAL) / MAX(applicable, 1) ASC, audit_day DESC LIMIT ?`).bind(...b, n).all();
  for (const r of rows) {
    const { results } = await d.prepare(
      "SELECT check_id, evidence, rewrite, failed_rules FROM cxa_check_results WHERE score_id = ? AND result = 'fail'"
    ).bind(r.id).all();
    r.fails = results.map((x) => ({ ...x, failed_rules: x.failed_rules ? JSON.parse(x.failed_rules) : [] }));
  }
  return rows;
}

export async function failures(env, f, { limit = 50, offset = 0 } = {}) {
  const d = db(env);
  const [cte, b] = scoresCte(f);
  const [cf, cb] = checkFilter(f);
  const lim = Math.max(1, Math.min(Number(limit) || 50, 200));
  const off = Math.max(0, Number(offset) || 0);
  const { results } = await d.prepare(`${cte}
    SELECT s.audit_day AS day, s.author_type, c.check_id, c.evidence, c.rewrite, c.failed_rules, c.reply_id,
      s.conversation_id, s.link, s.issue_type, s.channel, s.note, s.overall_note, s.guidelines_version
    FROM s JOIN cxa_check_results c ON c.score_id = s.id
    WHERE c.result = 'fail' ${cf}
    ORDER BY s.audit_day DESC, s.conversation_id, c.check_id
    LIMIT ? OFFSET ?`).bind(...b, ...cb, lim + 1, off).all();
  const more = results.length > lim;
  return {
    failures: results.slice(0, lim).map((r) => ({ ...r, failed_rules: r.failed_rules ? JSON.parse(r.failed_rules) : [] })),
    more, next_offset: more ? off + lim : null,
  };
}

// ---------------------------------------------------------------------------
// The fix list: failed checks grouped by (who, what, where it comes from),
// most frequent first, each with a suggested change and example links.
// Scores from before 2026-11-10 have no source: grouped under "Not recorded".
// ---------------------------------------------------------------------------
export const NOT_RECORDED = "Not recorded (scored before Nov 10)";

export async function fixList(env, f, { limit = 15 } = {}) {
  const d = db(env);
  const [cte, b] = scoresCte(f);
  const [cf, cb] = checkFilter(f);
  const { results } = await d.prepare(`${cte}
    SELECT s.audit_day AS day, s.author_type, s.conversation_id, s.link, s.issue_type,
      c.check_id, c.evidence, c.rewrite, c.failed_rules, c.source, c.fix
    FROM s JOIN cxa_check_results c ON c.score_id = s.id
    WHERE c.result = 'fail' ${cf}
    ORDER BY s.audit_day DESC, s.scored_at DESC`).bind(...b, ...cb).all();
  const groups = new Map();
  for (const r of results) {
    const source = r.source || NOT_RECORDED;
    const team = /^Team: /.test(source) ? source.slice(6) : null;
    // Right voice: one entry per broken rule (a reply can break two).
    const rules = r.check_id === "right_voice" && r.failed_rules ? JSON.parse(r.failed_rules) : [];
    for (const rule of rules.length ? rules : [null]) {
      const key = `${r.author_type}|${r.check_id}|${rule || ""}|${source}`;
      let g = groups.get(key);
      if (!g) {
        g = { author_type: r.author_type, who: r.author_type === "ivy" ? "Ivy" : team || "Team", check_id: r.check_id, rule, problem: problemFor(r.check_id, rule), source, count: 0, fix: "", examples: [] };
        groups.set(key, g);
      }
      g.count++;
      if (!g.fix && r.fix) g.fix = r.fix; // newest suggestion wins
      if (g.examples.length < 3) g.examples.push({ day: r.day, link: r.link, conversation_id: r.conversation_id, issue_type: r.issue_type, evidence: r.evidence, rewrite: r.rewrite });
    }
  }
  const fixes = [...groups.values()].sort((a, b) => b.count - a.count || (a.source === NOT_RECORDED) - (b.source === NOT_RECORDED));
  return { fixes: fixes.slice(0, limit), total_groups: fixes.length, total_fails: results.length };
}
