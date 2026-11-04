import { readFile } from "node:fs/promises";
export async function load(url, context, next) {
  if (url.endsWith(".md")) {
    const src = await readFile(new URL(url), "utf8");
    return { format: "module", source: `export default ${JSON.stringify(src)};`, shortCircuit: true };
  }
  return next(url, context);
}
