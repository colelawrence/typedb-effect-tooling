import Crypto from "node:crypto";

import * as FetchHttpClient from "@effect/platform/FetchHttpClient";
import * as NodeRuntime from "@effect/platform-node/NodeRuntime";
import * as Array from "effect/Array";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";

import { TypeDb } from "./client";
import { TelemetryLive } from "./telemetry";

const queries = [
  `
  define
  entity person;
    `,
  `
  define
  relation parentship, relates person, relates person;
  relation childship, relates person, relates person;

    `,
];

const MainLive = Layer.mergeAll(
  TypeDb.layer({
    username: "admin",
    password: "password",
    url: "http://localhost:8000",
  }).pipe(Layer.provide(FetchHttpClient.layer)),
);
const cleanup = Effect.gen(function* () {
  const client = yield* TypeDb;
  const dbs = yield* client.getDatabases;
  for (const db of dbs.databases) {
    if (db.name === "default") continue;

    yield* Effect.logWarning(`Deleting database ${db.name}`);
    yield* client.deleteDatabase(db.name);
  }
});

export const migrator = Effect.fn("migrator")(
  function* () {
    const client = yield* TypeDb;
    // yield* cleanup;

    // const dbName = `test-${Crypto.randomBytes(8).toString("hex")}`;
    const dbName = "test-db";
    yield* Effect.logInfo(`Creating database`, dbName);
    const database = yield* client.createDatabase(dbName);
    const { transactionId } = yield* client.openTransaction(dbName, "schema");

    for (const migration of queries) {
      const hash = Crypto.createHash("sha256").update(migration).digest("hex");
      yield* Effect.logInfo(`Running migration ${hash}`);
      yield* client.query({
        transactionId,
        query: migration,
      });
    }
    yield* client.commitTransaction(transactionId);
    const schema = yield* client.getDatabaseSchema(dbName);
    yield* Effect.logInfo(schema);

    const databases = yield* client.getDatabases;
    yield* Effect.logInfo(databases);
  },
  Effect.scoped,
  Effect.provide(MainLive),
);

NodeRuntime.runMain(migrator().pipe(Effect.provide(TelemetryLive)));
