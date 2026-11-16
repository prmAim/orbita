import { copyFile, mkdir, readdir } from "node:fs/promises";
const output = new URL(
  "../backend/src/main/resources/static/",
  import.meta.url,
);
await mkdir(output, { recursive: true });
for (const [source, target] of [
  ["src/app.css", "app.css"],
  ["src/index.html", "index.html"],
]) {
  await copyFile(new URL(source, import.meta.url), new URL(target, output));
}
for (const file of await readdir(new URL("dist/", import.meta.url))) {
  if (file.endsWith(".js"))
    await copyFile(new URL("dist/" + file, import.meta.url), new URL(file, output));
}
