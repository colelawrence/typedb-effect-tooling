import * as FetchHttpClient from "@effect/platform/FetchHttpClient";
import { BunFileSystem, BunRuntime } from "@effect/platform-bun";
import { command, option, positional, run, string, subcommands } from "cmd-ts";
import { File } from "cmd-ts/batteries/fs";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Runtime from "effect/Runtime";

import { Checker } from "./checker";
import { TypeDb } from "./client";
import { AppFileConfig } from "./config";
import { TelemetryLive } from "./telemetry";

const buildCommands = () =>
  Effect.gen(function* () {
    const runtime = yield* Effect.runtime();
    const runPromise = Runtime.runPromise(runtime);
    const checker = yield* Checker;

    const migrate = command({
      name: "migrate",
      version: "1.0.0",
      description: "migrations",
      args: {},
      handler: (args) => {
        console.log(args);
      },
    });

    const check = command({
      name: "check",
      version: "1.0.0",
      description: "check a file for errors",
      args: {
        database: option({
          long: "database",
          short: "d",
          description: "the database to check against",
          type: string,
          defaultValue: () => "default",
        }),
        file: positional({
          type: File,
          description: "the file to check",
          displayName: "file",
        }),
      },
      handler: async ({ file }) => {
        return runPromise(checker.checkFile(file));
      },
    });

    const cmd = subcommands({
      name: "my-command",
      description: "typedb-tools",
      version: "1.0.0",
      cmds: {
        migrate,
        check,
      },
    });
    yield* Effect.promise(() => run(cmd, process.argv.slice(2)));
  });

const CliLayer = Layer.mergeAll(Checker.Default).pipe(
  Layer.provide(
    TypeDb.layer({
      username: "admin",
      password: "password",
      url: "http://localhost:8000",
    }),
  ),
  Layer.provide(AppFileConfig.layer),
  Layer.provide(FetchHttpClient.layer),
  Layer.provide(BunFileSystem.layer),
);

const main = Effect.fn("cli")(function* () {
  yield* buildCommands();
}, Effect.provide(CliLayer));

BunRuntime.runMain(main().pipe(Effect.provide(TelemetryLive)));
