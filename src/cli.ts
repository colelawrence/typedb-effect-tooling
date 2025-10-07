import * as FetchHttpClient from "@effect/platform/FetchHttpClient";
import * as FileSystem from "@effect/platform/FileSystem";
import { BunFileSystem, BunRuntime } from "@effect/platform-bun";
import { command, option, positional, run, string, subcommands } from "cmd-ts";
import { File } from "cmd-ts/batteries/fs";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Runtime from "effect/Runtime";
import * as Schedule from "effect/Schedule";
import * as Stream from "effect/Stream";

import { Checker } from "./checker";
import { TypeDb } from "./client";
import { AppFileConfig, DatabaseConfig } from "./config";
import { TelemetryLive } from "./telemetry";

const buildCommands = () =>
  Effect.gen(function* () {
    const runtime = yield* Effect.runtime();
    const runPromise = Runtime.runPromise(runtime);
    const checker = yield* Checker;
    const client = yield* TypeDb;
    const fs = yield* FileSystem.FileSystem;

    const migrate = command({
      name: "migrate",
      version: "1.0.0",
      description: "migrations",
      args: {},
      handler: (args) => {
        console.log(args);
      },
    });

    const query = command({
      name: "query",
      description: "run a query from a file",
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
      handler: async ({ file, database }) => {
        return runPromise(
          Effect.gen(function* () {
            const { databaseName } = yield* DatabaseConfig;
            const query = yield* fs.readFileString(file);
            yield* Effect.logWarning(`Using database '${databaseName}'`);
            const res = yield* client.oneShotQuery({
              databaseName,
              query,
              transactionType: "read",
              commit: true,
            });
            if (res.answerType === "conceptRows") {
              for (const answer of res.answers) {
                for (const [key, value] of Object.entries(answer.data)) {
                  let formatted = `${key}: `;
                  switch (value?.kind) {
                    case "attribute": {
                      formatted += `${value.value} (${value.type.valueType})`;
                      break;
                    }
                    case "entity": {
                      formatted += `${value.iid}`;
                      break;
                    }
                    default:
                      formatted += `UNKNOWN ${value}`;
                  }
                  yield* Effect.logInfo(formatted);
                }
              }
            }
          }).pipe(
            Effect.provideService(DatabaseConfig, {
              databaseName: database,
            }),
          ),
        );
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
      handler: async ({ file, database }) =>
        runPromise(
          checker.checkFile(file).pipe(
            Effect.provideService(DatabaseConfig, {
              databaseName: database,
            }),
          ),
        ),
    });

    const cmd = subcommands({
      name: "my-command",
      description: "typedb-tools",
      version: "1.0.0",
      cmds: {
        migrate,
        check,
        query,
      },
    });
    yield* Effect.promise(() => run(cmd, process.argv.slice(2)));
  });

const CliLayer = Layer.mergeAll(Checker.Default).pipe(
  Layer.provideMerge(TypeDb.fromFileConfig),
  Layer.provide(AppFileConfig.layer),
  Layer.provide(FetchHttpClient.layer),
  Layer.provideMerge(BunFileSystem.layer),
);

// const client = yield* TypeDb;

// const stream = yield* Stream.repeatEffect(client.health).pipe(
//   Stream.schedule(Schedule.spaced("1 seconds")),
//   Stream.runForEach((x) => Effect.logInfo(x)),
// );
const main = Effect.fn("cli")(function* () {
  yield* buildCommands();
}, Effect.provide(CliLayer));

BunRuntime.runMain(main().pipe(Effect.provide(TelemetryLive)));
