import * as FileSystem from "@effect/platform/FileSystem";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";

import { SyntaxApiError, TypeDb } from "./client";
import { DatabaseConfig } from "./config";

// @effect-diagnostics-next-line leakingRequirements:off
export class Checker extends Effect.Service<Checker>()("app/Checker", {
  accessors: true,
  effect: Effect.gen(function* () {
    const client = yield* TypeDb;
    const fs = yield* FileSystem.FileSystem;
    const checkSyntax = Effect.fn("checkSyntax")(function* (code: string) {
      const { databaseName } = yield* DatabaseConfig;
      const tx = yield* client.openTransactionScoped(databaseName, "read");
      yield* Effect.logWarning(`Using database '${databaseName}'`);
      const result = yield* client
        .analyze(tx.transactionId, code)
        .pipe(
          Effect.catchSome((err) =>
            err instanceof SyntaxApiError
              ? Option.some(Effect.succeed(err))
              : Option.none(),
          ),
        );
      if (result instanceof SyntaxApiError) {
        yield* Effect.logError(result.message);
        return;
      }

      yield* Effect.logInfo(`✅ No errors`);
    }, Effect.scoped);
    const checkFile = Effect.fn("checkFile")(function* (filename: string) {
      const contents = yield* fs.readFileString(filename);
      return yield* checkSyntax(contents);
    });
    return {
      checkSyntax,
      checkFile,
    };
  }),
}) {}
