import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import {
  chmodSync,
  cpSync,
  existsSync,
  mkdirSync,
  mkdtempSync,
  readFileSync,
  readdirSync,
  rmSync,
  statSync,
  symlinkSync,
  writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import { test } from "node:test";
import { createHash } from "node:crypto";
import { pathToFileURL } from "node:url";

const script = path.resolve("scripts", "postinstall-patches.mjs");
const bin = path.resolve("node_modules", ".bin");
const packageName = "react-native-draggable-flatlist";
const patchFile = `${packageName}+4.0.3.patch`;

for (const platform of ["ios", "android"]) {
  test(`patched Markdown ships ${platform} props generated from its native specs`, () => {
    const packageDir = path.resolve("packages/app/node_modules/react-native-enriched-markdown");
    const output = mkdtempSync(path.join(tmpdir(), `markdown-${platform}-codegen-`));
    try {
      // includesGeneratedCode makes native builds use the shipped headers instead of
      // regenerating them after patch-package changes the TypeScript specs.
      const result = spawnSync(
        process.execPath,
        [
          path.resolve("node_modules/react-native/scripts/generate-codegen-artifacts.js"),
          "-p",
          packageDir,
          "-t",
          platform,
          "-o",
          output,
          "-s",
          "library",
        ],
        { encoding: "utf8" },
      );
      assert.equal(result.status, 0, result.stdout + result.stderr);
      const header =
        platform === "ios"
          ? "ReactCodegen/EnrichedMarkdownTextSpec/Props.h"
          : "jni/react/renderer/components/EnrichedMarkdownTextSpec/Props.h";
      assert.equal(
        readFileSync(path.join(packageDir, platform, "generated", header), "utf8"),
        readFileSync(path.join(output, header), "utf8"),
      );
    } finally {
      rmSync(output, { recursive: true, force: true });
    }
  });
}

function createCheckout() {
  const root = mkdtempSync(path.join(tmpdir(), "postinstall-patches-"));
  writeFileSync(path.join(root, "package.json"), JSON.stringify({ name: "checkout" }));
  const packageDir = path.join(root, "node_modules", packageName);
  mkdirSync(packageDir, { recursive: true });
  writeFileSync(
    path.join(packageDir, "package.json"),
    JSON.stringify({ name: packageName, version: "4.0.3" }),
  );
  writeFileSync(path.join(packageDir, "index.js"), "export const value = 1;\n");
  mkdirSync(path.join(root, "patches"));
  return { root, packageDir };
}

function writePatch(root, from, to) {
  const file = `node_modules/${packageName}/index.js`;
  writeFileSync(
    path.join(root, "patches", patchFile),
    [
      `diff --git a/${file} b/${file}`,
      `--- a/${file}`,
      `+++ b/${file}`,
      "@@ -1 +1 @@",
      `-export const value = ${from};`,
      `+export const value = ${to};`,
      "",
    ].join("\n"),
  );
}

function install(root, PATH = `${bin}${path.delimiter}${process.env.PATH}`) {
  return spawnSync(process.execPath, [script], {
    cwd: root,
    encoding: "utf8",
    env: { ...process.env, PATH },
  });
}

test("a patch that changed since it was applied removes the package for a clean reinstall", () => {
  const { root, packageDir } = createCheckout();
  try {
    writePatch(root, 1, 2);
    assert.equal(install(root).status, 0);
    assert.equal(
      readFileSync(path.join(packageDir, "index.js"), "utf8"),
      "export const value = 2;\n",
    );

    writePatch(root, 1, 3);
    const result = install(root);

    assert.equal(result.status, 1);
    assert.match(result.stderr, /Run npm install again/);
    assert.equal(existsSync(packageDir), false);
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

test("an already applied patch leaves the package in place", () => {
  const { root, packageDir } = createCheckout();
  try {
    writePatch(root, 1, 2);
    assert.equal(install(root).status, 0);
    assert.equal(install(root).status, 0);
    assert.equal(
      readFileSync(path.join(packageDir, "index.js"), "utf8"),
      "export const value = 2;\n",
    );
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

test("a missing patch-package fails the install and leaves the package in place", () => {
  const { root, packageDir } = createCheckout();
  try {
    writePatch(root, 1, 2);
    const result = install(root, root);

    assert.equal(result.status, 1);
    assert.match(result.stderr, /failed to spawn/);
    assert.equal(existsSync(packageDir), true);
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

const serverFixture = path.resolve("scripts/fixtures/server-0.15.4-query.fixture");
const serverReplacement = path.resolve("patches/server-0.15.4-query.js");
const serverSource = process.env.PASEO_SERVER_SOURCE_DIR
  ? path.resolve(process.env.PASEO_SERVER_SOURCE_DIR)
  : path.resolve("scripts/fixtures/server-0.15.4-runtime");
const serverSourceDist = process.env.PASEO_SERVER_SOURCE_DIR
  ? path.join(serverSource, "dist")
  : path.join(serverSource, "compiled");
const serverQuery = "dist/server/server/agent/providers/claude/query.js";
function digest(bytes) {
  return createHash("sha256").update(bytes).digest("hex");
}
function createServerTarget(version = "0.15.4", queryBytes = readFileSync(serverFixture)) {
  const root = mkdtempSync(path.join(process.env.TMPDIR ?? tmpdir(), "paseo-server-patch-"));
  mkdirSync(path.join(root, "dist/server/server/agent/providers/claude"), { recursive: true });
  writeFileSync(
    path.join(root, "package.json"),
    JSON.stringify({ name: "@getpaseo/server", version }),
  );
  writeFileSync(path.join(root, serverQuery), queryBytes);
  return root;
}
function applyServerTarget(root, args = ["--paseo-server-dir", root]) {
  return spawnSync(process.execPath, [script, ...args], { encoding: "utf8" });
}

test("the explicit server target applies the verified 0.15.4 Claude launcher idempotently", () => {
  const root = createServerTarget();
  try {
    const originalPath = path.join(root, serverQuery);
    chmodSync(originalPath, 0o640);
    const applied = applyServerTarget(root);
    assert.equal(applied.status, 0, applied.stderr);
    const patchedBytes = readFileSync(originalPath);
    assert.deepEqual(patchedBytes, readFileSync(serverReplacement));
    assert.equal(statSync(originalPath).mode & 0o777, 0o640);
    assert.equal(applyServerTarget(root).status, 0);
    assert.deepEqual(readFileSync(originalPath), patchedBytes);
    assert.equal(
      readdirSync(path.dirname(originalPath)).some((name) => name.endsWith(".tmp")),
      false,
    );
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

test("the explicit server target rejects unknown bytes and versions without mutation", () => {
  for (const [version, bytes, args] of [
    ["0.15.4", Buffer.from("corrupt"), undefined],
    ["0.15.3", readFileSync(serverFixture), undefined],
    ["0.15.4", readFileSync(serverFixture), ["--unexpected", "--paseo-server-dir", "target"]],
    ["0.15.4", readFileSync(serverFixture), ["--paseo-server-dir", "target", "--extra"]],
  ]) {
    const root = createServerTarget(version, bytes);
    try {
      const before = readFileSync(path.join(root, serverQuery));
      const result = applyServerTarget(root, args ?? ["--paseo-server-dir", root]);
      assert.equal(result.status, 1);
      assert.deepEqual(readFileSync(path.join(root, serverQuery)), before);
      assert.equal(digest(readFileSync(path.join(root, serverQuery))), digest(before));
    } finally {
      rmSync(root, { recursive: true, force: true });
    }
  }
});

test("Claude launch keeps MCP credentials out of argv and cleans its private config", async () => {
  const root = mkdtempSync(path.join(process.env.TMPDIR ?? tmpdir(), "claude-launch-boundary-"));
  try {
    const sourcePackage = JSON.parse(readFileSync(path.join(serverSource, "package.json"), "utf8"));
    cpSync(serverSourceDist, path.join(root, "dist"), { recursive: true });
    if (!process.env.PASEO_SERVER_SOURCE_DIR) {
      writeFileSync(path.join(root, serverQuery), readFileSync(serverFixture));
    }
    writeFileSync(
      path.join(root, "package.json"),
      JSON.stringify({ name: "@getpaseo/server", version: sourcePackage.version, type: "module" }),
    );
    symlinkSync(path.resolve("node_modules"), path.join(root, "node_modules"), "junction");
    assert.equal(applyServerTarget(root).status, 0);
    const probePath = path.join(root, "probe.cjs");
    const resultPath = path.join(root, "probe-result.json");
    const fakeToken = "FAKE_ACCEPTANCE_TOKEN_9f2";
    writeFileSync(
      probePath,
      `const fs = require("node:fs");
` +
        `const path = require("node:path");
` +
        `const args = process.argv;
` +
        `const configPath = args[args.indexOf("--mcp-config") + 1];
` +
        `const config = JSON.parse(fs.readFileSync(configPath, "utf8"));
` +
        `const stat = fs.statSync(configPath);
` +
        `fs.writeFileSync(args[2], JSON.stringify({
` +
        `  argvContainsCredential: args.some((arg) => arg.includes(${JSON.stringify(fakeToken)})),
` +
        `  configPreserved: config.mcpServers.paseo.headers.Authorization === ${JSON.stringify(`Bearer ${fakeToken}`)},
` +
        `  fileMode: stat.mode & 0o777,
` +
        `  parentMode: fs.statSync(path.dirname(configPath)).mode & 0o777,
` +
        `  configDirectory: path.dirname(configPath),
` +
        `}));
`,
      { mode: 0o600 },
    );

    const { claudeQuery } = await import(pathToFileURL(path.join(root, serverQuery)).href);
    const controller = new AbortController();
    const child = claudeQuery(
      { options: {} },
      {
        queryFactory: ({ options }) =>
          options.spawnClaudeCodeProcess({
            command: process.execPath,
            args: [
              probePath,
              resultPath,
              "--mcp-config",
              JSON.stringify({
                mcpServers: { paseo: { headers: { Authorization: `Bearer ${fakeToken}` } } },
              }),
            ],
            cwd: root,
            env: process.env,
            signal: controller.signal,
          }),
      },
    );
    const exitCode = await new Promise((resolve, reject) => {
      child.once("error", reject);
      child.once("exit", (code) => resolve(code));
    });
    assert.equal(exitCode, 0);
    const result = JSON.parse(readFileSync(resultPath, "utf8"));
    assert.deepEqual(
      {
        argvContainsCredential: result.argvContainsCredential,
        configPreserved: result.configPreserved,
        fileMode: result.fileMode,
        parentMode: result.parentMode,
      },
      {
        argvContainsCredential: false,
        configPreserved: true,
        fileMode: 0o600,
        parentMode: 0o700,
      },
    );
    assert.equal(existsSync(result.configDirectory), false);
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});
