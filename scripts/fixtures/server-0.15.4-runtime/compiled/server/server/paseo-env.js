const PASEO_NODE_ENV = "PASEO_NODE_ENV";
const ELECTRON_RUN_AS_NODE = "ELECTRON_RUN_AS_NODE";
const RUNTIME_CONTROL_ENV_KEYS = [
  PASEO_NODE_ENV,
  "PASEO_DESKTOP_MANAGED",
  "PASEO_SUPERVISED",
  ELECTRON_RUN_AS_NODE,
  "ELECTRON_NO_ATTACH_CONSOLE",
  "ESBUILD_BINARY_PATH",
];
function buildInternalProcessEnv(baseEnv) {
  return { ...baseEnv };
}
function buildExternalProcessEnv(baseEnv, overlays) {
  const sanitized = Object.assign({}, baseEnv, ...overlays);
  for (const key of RUNTIME_CONTROL_ENV_KEYS) {
    delete sanitized[key];
  }
  for (const [key, value] of Object.entries(sanitized)) {
    if (value === undefined) {
      delete sanitized[key];
    }
  }
  return sanitized;
}
export function createPaseoInternalEnv(baseEnv) {
  return buildInternalProcessEnv(baseEnv);
}
export function createExternalProcessEnv(baseEnv, ...overlays) {
  return buildExternalProcessEnv(baseEnv, overlays);
}
export function createExternalCommandProcessEnv(_command, baseEnv, ...overlays) {
  // Deprecated command parameter: retained while callers migrate to createExternalProcessEnv.
  return buildExternalProcessEnv(baseEnv, overlays);
}
export function buildSelfNodeCommand(args, envOverlay) {
  const env = buildExternalProcessEnv(process.env, []);
  Object.assign(env, { [ELECTRON_RUN_AS_NODE]: "1" }, envOverlay);
  for (const [key, value] of Object.entries(env)) {
    if (value === undefined) {
      delete env[key];
    }
  }
  return {
    command: process.execPath,
    args,
    env,
  };
}
export function resolvePaseoNodeEnv(env) {
  const value = env[PASEO_NODE_ENV];
  return value === "development" || value === "production" || value === "test" ? value : undefined;
}
//# sourceMappingURL=paseo-env.js.map
