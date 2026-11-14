import { readdirSync } from "node:fs";
import { extname, join } from "node:path";
function hasPathSeparator(value) {
  return value.includes("/") || value.includes("\\");
}
function enumerateLiteralPathCandidates(executablePath) {
  if (extname(executablePath)) {
    return [executablePath];
  }
  return [executablePath, `${executablePath}.exe`, `${executablePath}.cmd`];
}
function enumerateWingetPackageCandidates(name, localAppData) {
  if (!localAppData) {
    return [];
  }
  const wingetPackages = join(localAppData, "Microsoft", "WinGet", "Packages");
  let packageDirs;
  try {
    packageDirs = readdirSync(wingetPackages, { withFileTypes: true })
      .filter((entry) => entry.isDirectory())
      .map((entry) => entry.name);
  } catch {
    return [];
  }
  const exeName = `${name}.exe`;
  return packageDirs.map((packageDir) => join(wingetPackages, packageDir, exeName));
}
async function find(input, options) {
  if (hasPathSeparator(input)) {
    return findFirstProbeable(enumerateLiteralPathCandidates(input), options);
  }
  const pathCandidates = await options.enumeratePathCandidates(input);
  const wingetCandidates = enumerateWingetPackageCandidates(
    input,
    options.localAppData ?? process.env.LOCALAPPDATA,
  ).filter(options.exists);
  return findFirstProbeable([...pathCandidates, ...wingetCandidates], options);
}
async function findFirstProbeable(candidates, options) {
  const seen = new Set();
  for (const candidate of candidates) {
    if (seen.has(candidate)) {
      continue;
    }
    seen.add(candidate);
    if (!options.exists(candidate)) {
      continue;
    }
    if (await options.probeExecutable(candidate, options.probeTimeoutMs)) {
      return candidate;
    }
  }
  return null;
}
function exists(executablePath, options) {
  for (const candidate of enumerateLiteralPathCandidates(executablePath)) {
    if (options.exists(candidate)) {
      return candidate;
    }
  }
  return null;
}
export const windowsExecutableResolution = {
  exists,
  find,
};
//# sourceMappingURL=windows.js.map
