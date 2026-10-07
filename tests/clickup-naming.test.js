// Tests for how the ClickUp automation turns a task title into a folder / task
// name. Invented fixtures only.
//
// The rule that broke: under task-id naming, a title the team wrote starting
// with an old code ("2025.753 - Preeclampsia repeat") was mistaken for one of
// our own prefixes, so the code was stripped and the task never renamed.
import { test } from "node:test";
import assert from "node:assert/strict";
import {
  splitTaskName,
  templatesWantCode,
  buildVars,
  folderNameFor,
} from "../src/clickup-automation.js";

const TASK = { id: "86abc1def", custom_id: null };
const BY_ID = { folder_name_template: "{task_id} - {name}", subfolders: "[]" };
const BY_CODE = { folder_name_template: "{code} - {name}", subfolders: "[]" };

function nameFor(automation, title, task = TASK, code = null) {
  const wantsCode = templatesWantCode(automation);
  const parsed = splitTaskName(title, { wantsCode, task });
  return folderNameFor(automation, buildVars({ code: parsed.code || code, name: parsed.clean, task }));
}

test("task-id naming keeps a leading code as part of the title", () => {
  assert.equal(
    nameFor(BY_ID, "2025.753 - Preeclampsia repeat"),
    "86abc1def - 2025.753 - Preeclampsia repeat"
  );
});

test("task-id naming never treats someone's code as ours, so the task is renamed", () => {
  const parsed = splitTaskName("2025.753 - Preeclampsia repeat", { wantsCode: false, task: TASK });
  assert.equal(parsed.hadCode, false);
  assert.equal(parsed.clean, "2025.753 - Preeclampsia repeat");
});

test("a plain title gets the id on the front", () => {
  assert.equal(nameFor(BY_ID, "Spring Sale Video"), "86abc1def - Spring Sale Video");
});

test("re-running on an already-renamed task does not double the prefix", () => {
  const once = nameFor(BY_ID, "2025.753 - Preeclampsia repeat");
  assert.equal(nameFor(BY_ID, once), once);
});

test("only the task's own id is stripped, not a different id that happens to match", () => {
  const other = { id: "86abc", custom_id: null };
  assert.equal(nameFor(BY_ID, "86abc1def - Notes", other), "86abc - 86abc1def - Notes");
});

test("a custom task id prefix is recognised as ours too", () => {
  const task = { id: "86abc1def", custom_id: "GOOG-12" };
  const auto = { folder_name_template: "{custom_id} - {name}", subfolders: "[]" };
  assert.equal(nameFor(auto, "GOOG-12 - Launch", task), "GOOG-12 - Launch");
});

test("code naming still keeps an existing code and does not re-number it", () => {
  const parsed = splitTaskName("2026.0012 - Launch", { wantsCode: true, task: TASK });
  assert.deepEqual(parsed, { code: "2026.0012", clean: "Launch", hadCode: true });
  assert.equal(nameFor(BY_CODE, "2026.0012 - Launch"), "2026.0012 - Launch");
});

test("a code is only wanted when some template actually uses {code}", () => {
  assert.equal(templatesWantCode(BY_ID), false);
  assert.equal(templatesWantCode(BY_CODE), true);
  assert.equal(
    templatesWantCode({ folder_name_template: "{task_id} - {name}", subfolders: '["{code} raw"]' }),
    true
  );
});
