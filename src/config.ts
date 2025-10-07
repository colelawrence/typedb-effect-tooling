import * as FileSystem from "@effect/platform/FileSystem";
import { YAML } from "bun";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";

export class FileConfigSchema extends Schema.Class<FileConfigSchema>(
  "ConfigSchema",
)({
  typedb_url: Schema.URL,
  typedb_username: Schema.String,
  typedb_password: Schema.String.pipe(Schema.Redacted),
  schemas: Schema.Array(Schema.String),
}) {}

export class AppFileConfig extends Context.Tag("AppFileConfig")<
  AppFileConfig,
  FileConfigSchema
>() {
  static layer = Layer.effect(
    this,
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const contents = yield* fs.readFileString("typedb.phosphor-config.yaml");
      const yaml = YAML.parse(contents);
      return yield* Schema.decodeUnknown(FileConfigSchema)(yaml);
    }),
  );
}
