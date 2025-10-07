import * as Cause from "effect/Cause";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import type * as Redacted from "effect/Redacted";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";

import { TypeDb } from "./client";

interface CoreConfigurationDefinition {
  httpUrl: URL;
  username: string;
  password: Redacted.Redacted<string>;
  debugName: string;
  schemaSources: ReadonlyArray<{
    sourceKey: string;
    sourceCode: string;
  }>;
}

export class CoreConfiguration extends Context.Tag("CoreConfiguration")<
  CoreConfiguration,
  CoreConfigurationDefinition
>() {}

export class SourceNote extends Schema.TaggedStruct("SourceNote", {
  /** 0 based line number */
  line: Schema.Number.pipe(Schema.optional),
  /** 0 based column number */
  column: Schema.Number.pipe(Schema.optional),
  message: Schema.String,
  severity: Schema.Literal("error", "warning", "info"),
}) {}

export class SchemaSourceError extends Schema.TaggedError<SchemaSourceError>(
  "SchemaSourceError",
)("SchemaSourceError", {
  sourceKey: Schema.String,
  sourceCode: Schema.String,
  notes: Schema.Array(SourceNote),
}) {}

export class CoreService extends Effect.Service<CoreService>()(
  "app CoreService",
  {
    accessors: true,
    scoped: Effect.gen(function* () {
      const config = yield* CoreConfiguration;
      const typedb = yield* TypeDb.make({
        url: config.httpUrl,
        username: config.username,
        password: config.password,
      });

      const databaseName = yield* typedb.createDatabaseScoped(
        `temp_${config.debugName.slice(0, 16).replace(/[^a-zA-Z0-9]+/g, "_")}_${Date.now().toString(36)}`,
      );

      for (const schemaSource of config.schemaSources) {
        yield* Effect.scoped(
          Effect.gen(function* () {
            const tx = yield* typedb.openTransaction(databaseName, "schema");
            yield* typedb.query({
              transactionId: tx.transactionId,
              query: schemaSource.sourceCode,
            });
            yield* typedb.commitTransaction(tx.transactionId);
          }),
        ).pipe(
          Effect.catchAllCause((cause) => {
            return new SchemaSourceError({
              sourceKey: schemaSource.sourceKey,
              sourceCode: schemaSource.sourceCode,
              notes: [
                SourceNote.make({
                  message: Cause.pretty(cause),
                  severity: "error",
                }),
              ],
            });
          }),
        );
      }

      return {
        check: Effect.fn("TypeDB.check")(function* (sourceCode: string) {
          // TODO: use database
        }),
        // healthcheck: Stream.repeatEffect(typedb.health).pipe(
        //   Stream.map(() => true),
        // ),
        // checkConnection: Effect.gen(function* () {
        //   yield* typedb.getDatabases;
        //   return true;
        // }),
      };
    }),
  },
) {}
