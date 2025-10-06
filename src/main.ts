import * as FetchHttpClient from "@effect/platform/FetchHttpClient";
import { NodeRuntime } from "@effect/platform-node";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import { AnalyzeResponse } from "typedb-driver-http";

import { TypeDb } from "./client";
import { TelemetryLive } from "./telemetry";

const Main = TypeDb.layer({
  username: "admin",
  password: "password",
  url: "http://localhost:8000",
}).pipe(Layer.provide(Layer.mergeAll(FetchHttpClient.layer, TelemetryLive)));

const main = Effect.gen(function* () {
  const typedb = yield* TypeDb;
  const user = yield* typedb.getCurrentUser;
  const databases = yield* typedb.getDatabases;
  const transaction = yield* typedb.openTransaction("default", "read");
  const res: AnalyzeResponse = yield* typedb.analyze(
    transaction.transactionId,
    `match
    $user isa user;
    reduce $count = count;`,
  );
  yield* Effect.logInfo(JSON.stringify(res, null, 2));
  yield* typedb.closeTransaction(transaction.transactionId);
  yield* Effect.logInfo(transaction);
}).pipe(Effect.provide(Main));

NodeRuntime.runMain(main);
