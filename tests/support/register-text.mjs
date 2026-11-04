// Lets `node --test` import .md files as text, the way wrangler.jsonc's
// "rules" makes the Worker bundle them (src/nitm-pm imports its instructions).
import { register } from "node:module";
register("./text-hooks.mjs", import.meta.url);
