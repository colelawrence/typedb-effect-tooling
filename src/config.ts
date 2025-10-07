import * as FileSystem from "@effect/platform/FileSystem";
import { YAML } from "bun";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";

export class FileConfigSchema extends Schema.Class<FileConfigSchema>(
  "ConfigSchema",
)({
  typedb_http_url: Schema.URL,
  typedb_username: Schema.String,
  typedb_password: Schema.String.pipe(Schema.Redacted),
  schemas: Schema.Array(Schema.String),
}) {}

export class FileConfigError extends Schema.TaggedError<FileConfigError>()(
  "FileConfigError",
  {
    filePath: Schema.String,
    cause: Schema.Defect,
  },
) {}

export class AppFileConfig extends Context.Tag("AppFileConfig")<
  AppFileConfig,
  FileConfigSchema
>() {
  static layer = Layer.effect(
    this,
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      // TODO: Add walking up the directory tree to find the config file
      // - "memoize" AppFileConfig per filePath + lastModifiedTimestamp resolved from a input typedb config file
      const filePath = "typedb.phosphor-config.yaml";
      const contents = yield* fs.readFileString(filePath);
      const yaml = yield* Effect.try({
        try: () => YAML.parse(contents),
        catch: (error) => new FileConfigError({ filePath, cause: error }),
      });
      return yield* Schema.decodeUnknown(FileConfigSchema)(yaml);
    }).pipe(Effect.orDie),
  );
}

export interface DatabaseConfigDefinition {
  databaseName: string;
}

export class DatabaseConfig extends Context.Tag("DatabaseConfig")<
  DatabaseConfig,
  DatabaseConfigDefinition
>() {}
