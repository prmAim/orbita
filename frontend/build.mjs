import { copyFile, mkdir } from "node:fs/promises";
const output = new URL(
  "../backend/src/main/resources/static/",
  import.meta.url,
);
await mkdir(output, { recursive: true });
for (const [source, target] of [
  ["dist/app.js", "app.js"],
  ["src/app.css", "app.css"],
  ["src/index.html", "index.html"],
]) {
  await copyFile(new URL(source, import.meta.url), new URL(target, output));
}
