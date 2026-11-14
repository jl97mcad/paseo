import { spawnProcess as spawnCli, execCommand as execCli } from "@getpaseo/plugin/server";
import { createExternalCommandProcessEnv } from "../server/paseo-env.js";
export function spawnProcess(command, args, options) {
  const { baseEnv, env, envOverlay, ...spawnOptions } = options ?? {};
  const resolvedBaseEnv = env ?? baseEnv ?? process.env;
  const childEnv =
    options?.envMode === "internal"
      ? { ...resolvedBaseEnv, ...envOverlay }
      : createExternalCommandProcessEnv(
          command,
          resolvedBaseEnv,
          ...(envOverlay ? [envOverlay] : []),
        );
  return spawnCli(command, args, {
    ...spawnOptions,
    env: childEnv,
    signal: options?.signal,
  });
}
export async function execCommand(command, args, options) {
  const { baseEnv, env, envOverlay } = options ?? {};
  const resolvedBaseEnv = env ?? baseEnv ?? process.env;
  const childEnv =
    options?.envMode === "internal"
      ? { ...resolvedBaseEnv, ...envOverlay }
      : createExternalCommandProcessEnv(
          command,
          resolvedBaseEnv,
          ...(envOverlay ? [envOverlay] : []),
        );
  return execCli(command, args, {
    cwd: options?.cwd,
    env: childEnv,
    shell: options?.shell,
    encoding: options?.encoding ?? "utf8",
    signal: options?.signal,
    killSignal: options?.killSignal,
    timeout: options?.timeout,
    maxBuffer: options?.maxBuffer,
  });
}
//# sourceMappingURL=spawn.js.map
