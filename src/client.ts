import * as Headers from "@effect/platform/Headers";
import * as HttpBody from "@effect/platform/HttpBody";
import * as HttpClient from "@effect/platform/HttpClient";
import * as HttpClientError from "@effect/platform/HttpClientError";
import * as HttpClientRequest from "@effect/platform/HttpClientRequest";
import { HttpMethod } from "@effect/platform/HttpMethod";
import * as Cache from "effect/Cache";
import * as Context from "effect/Context";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Match from "effect/Match";
import * as Option from "effect/Option";
import * as Redacted from "effect/Redacted";
import * as Schema from "effect/Schema";
import { decodeJwt } from "jose";
import {
  AnalyzeResponse,
  QueryOptions,
  TransactionOptions,
  TransactionType,
} from "typedb-driver-http";

import { AppFileConfig } from "./config";

export class GenericApiError extends Schema.Class<GenericApiError>(
  "GenericApiError",
)({
  code: Schema.String,
  message: Schema.String,
}) {}

export class SyntaxApiError extends Schema.Class<SyntaxApiError>("SyntaxError")(
  {
    code: Schema.Literal("TQL0"),
    message: Schema.String,
  },
) {}

export const ApiError = Schema.Union(SyntaxApiError, GenericApiError);
export type ApiError = typeof ApiError.Type;

export class TypeDbError extends Schema.TaggedError<TypeDbError>()(
  "TypeDbError",
  {
    cause: Schema.Defect,
  },
) {}

export class TokenResponse extends Schema.Class<TokenResponse>("TokenResponse")(
  {
    token: Schema.String,
  },
) {}

export class DecodedToken extends Schema.Class<DecodedToken>("DecodedToken")({
  sub: Schema.String,
  iat: Schema.Number,
  exp: Schema.Number,
}) {}

interface TypeDbConfig {
  username: string;
  password: string;
  url: string | URL;
}

export type OneShotQueryArgs = {
  query: string;
  commit: boolean;
  databaseName: string;
  transactionType: TransactionType;
  transactionOptions?: TransactionOptions;
  queryOptions?: QueryOptions;
};

export type QueryArgs = {
  transactionId: string;
  query: string;
  queryOptions?: QueryOptions;
};

const make = ({ username, password, url }: TypeDbConfig) =>
  Effect.gen(function* () {
    // We need a "public" client to do the authentication
    const publicHttp = yield* HttpClient.HttpClient;
    const authenticate = Effect.fn("TypeDb: signIn")(
      function* ({
        username,
        password,
      }: Pick<TypeDbConfig, "username" | "password">) {
        return yield* publicHttp
          .post(new URL("/v1/signin", url), {
            body: yield* HttpBody.json({
              username,
              password,
            }),
          })
          .pipe(
            Effect.flatMap((_) => _.json),
            Effect.flatMap(Schema.decodeUnknown(TokenResponse)),
          );
      },
      Effect.catchTag("ParseError", Effect.orDie),
    );

    const tokenCache = yield* Cache.makeWith({
      capacity: 1,
      lookup: authenticate,
      timeToLive: Exit.match({
        onFailure: () => Duration.zero,
        onSuccess: (token) => {
          const decoded = Schema.decodeUnknownSync(DecodedToken)(
            decodeJwt(token.token),
          );
          const now = Date.now() / 1000;
          const remaining = Duration.seconds(decoded.exp - now - 2);
          return remaining;
        },
      }),
    });

    // NOTE: Passing inline (directly as argument to `tokenCache.get`) means we get a new object instance
    // every time, so we fetch a new token every time
    const creds = { username, password };

    const http = yield* HttpClient.HttpClient.pipe(
      Effect.map(
        HttpClient.mapRequestEffect((req) =>
          tokenCache
            .get(creds)
            .pipe(
              Effect.map((token) =>
                req.pipe(HttpClientRequest.bearerToken(token.token)),
              ),
            ),
        ),
      ),
    );

    /**
     * Creates a generic HTTP method handler for TypeDB API endpoints.
     *
     * This is the core method factory used internally to create all TypeDB API method calls.
     * It handles authentication, request/response processing, error handling, and response parsing
     * based on content type.
     *
     * Usage example:
     * ```typescript
     * const getUserMethod = makeMethod(
     *   "getUser",
     *   "GET",
     *   "/v1/users/john",
     *   Schema.Struct({ username: Schema.String }),
     * );
     *
     * const createUserMethod = makeMethod(
     *   "createUser",
     *   "POST",
     *   "/v1/users",
     *   Schema.Void,  // Void used for empty responses
     *   { username: "john", password: "secret" } // body for POST request
     * );
     * ```
     *
     * @param name - Human-readable name for the operation (used in tracing/logging)
     * @param method - HTTP method to use (GET, POST, DELETE, etc.)
     * @param path - API endpoint path relative to the base URL
     * @param schema - Effect Schema used to decode and validate the response
     * @param body - Optional request body object (will be JSON serialized)
     * @returns An Effect that executes the HTTP request and returns the decoded response
     *
     * @throws {TypeDbError} When the API returns an error response or unknown content type
     * @throws {ParseError} Schema validation errors are converted to defects via Effect.die
     */
    const makeMethod = <A, I, R, B extends object>(
      name: string,
      method: HttpMethod,
      path: string,
      schema: Schema.Schema<A, I, R>,
      body?: B,
    ) =>
      Effect.gen(function* () {
        // const b = yield* body === undefined
        //   ? Effect.succeed(undefined)
        //   : HttpBody.json(body);
        const b = yield* Option.fromNullable(body).pipe(
          Effect.flatMap(HttpBody.json),
          Effect.catchTag("NoSuchElementException", () =>
            Effect.succeed(undefined),
          ),
        );
        const res = yield* http.execute(
          HttpClientRequest.make(method)(new URL(path, url), {
            body: b,
          }),
        );
        if (res.status !== 200) {
          const error = yield* res.json.pipe(
            Effect.flatMap(Schema.decodeUnknown(ApiError)),
          );
          return yield* Effect.fail(error);
        }
        const contentType = Headers.get(res.headers, "content-type");

        // NOTE: When TypeDB doesn't return a content-type, it's an empty "OK" response
        // In those cases we have passed a Schema.Void,
        if (Option.isNone(contentType)) {
          return yield* res.json.pipe(
            Effect.flatMap(Schema.decodeUnknown(schema)),
          );
        }

        return yield* Match.value(contentType.value).pipe(
          Match.when(
            (s) => s.startsWith("text/plain"),
            () => res.text,
          ),
          Match.when(
            (s) => s.startsWith("application/json"),
            () => res.json,
          ),
          Match.orElse(() =>
            Effect.fail(
              new TypeDbError({
                cause: `Unknown content type: ${contentType.value}`,
              }),
            ),
          ),
          Effect.flatMap(Schema.decodeUnknown(schema)),
        );
      }).pipe(
        Effect.catchTag("ParseError", Effect.die),
        Effect.withSpan(`TypeDb: ${name}`),
      );

    const oneShotQuery = (args: OneShotQueryArgs) =>
      makeMethod("oneShotQuery", "POST", `/v1/query`, Schema.Any, args);

    const query = ({ transactionId, ...rest }: QueryArgs) =>
      makeMethod(
        "query",
        "POST",
        `/v1/transactions/${transactionId}/query`,
        Schema.Any,
        rest,
      );

    const openTransaction = (
      databaseName: string,
      transactionType: TransactionType,
    ) =>
      makeMethod(
        "openTransaction",
        "POST",
        `/v1/transactions/open`,
        Schema.Struct({
          transactionId: Schema.String,
        }),
        {
          databaseName,
          transactionType,
        },
      );

    const closeTransaction = (transactionId: string) =>
      makeMethod(
        "closeTransaction",
        "POST",
        `/v1/transactions/${transactionId}/close`,
        Schema.Void,
      );

    const commitTransaction = (transactionId: string) =>
      makeMethod(
        "commitTransaction",
        "POST",
        `/v1/transactions/${transactionId}/commit`,
        Schema.Void,
      );

    const rollbackTransaction = (transactionId: string) =>
      makeMethod(
        "rollbackTransaction",
        "POST",
        `/v1/transactions/${transactionId}/rollback`,
        Schema.Void,
      );

    const openTransactionScoped = (dbName: string, txType: TransactionType) =>
      Effect.acquireRelease(openTransaction(dbName, txType), (tx, exit) =>
        Exit.match(exit, {
          onFailure: () =>
            closeTransaction(tx.transactionId).pipe(Effect.orDie),
          onSuccess: () =>
            Match.value(txType).pipe(
              Match.when("read", () => closeTransaction(tx.transactionId)),
              Match.when("write", () => commitTransaction(tx.transactionId)),
              Match.when("schema", () => commitTransaction(tx.transactionId)),
              Match.exhaustive,
              Effect.orDie,
            ),
        }),
      ).pipe(
        Effect.annotateSpans({
          txType,
        }),
      );

    const analyze = (
      transactionId: string,
      query: string,
    ): Effect.Effect<
      AnalyzeResponse,
      | HttpClientError.HttpClientError
      | HttpBody.HttpBodyError
      | TypeDbError
      | SyntaxApiError
      | GenericApiError
    > =>
      makeMethod(
        "analyze",
        "POST",
        `/v1/transactions/${transactionId}/analyze`,
        Schema.Any,
        {
          query,
        },
      );

    const getCurrentUser = makeMethod(
      "getCurrentUser",
      "GET",
      `/v1/users/${username}`,
      Schema.Struct({
        username: Schema.String,
      }),
    );

    const getDatabases = makeMethod(
      "getDatabases",
      "GET",
      `/v1/databases`,
      Schema.Struct({
        databases: Schema.Array(
          Schema.Struct({
            name: Schema.String,
          }),
        ),
      }),
    );

    const createDatabase = (databaseName: string) =>
      makeMethod(
        "createDatabase",
        "POST",
        `/v1/databases/${databaseName}`,
        Schema.Void,
      );

    const deleteDatabase = (databaseName: string) =>
      makeMethod(
        "deleteDatabase",
        "DELETE",
        `/v1/databases/${databaseName}`,
        Schema.Void,
      );

    const createDatabaseScoped = (dbName: string) =>
      Effect.acquireRelease(
        createDatabase(dbName).pipe(
          Effect.andThen(() => Effect.succeed(dbName)),
        ),
        () => deleteDatabase(dbName).pipe(Effect.orDie),
      ).pipe(Effect.withSpan("createDatabaseScoped"));

    const getDatabaseSchema = (databaseName: string) =>
      makeMethod(
        "getDatabaseSchema",
        "GET",
        `/v1/databases/${databaseName}/schema`,
        Schema.String,
      );
    const getDatabaseTypeSchema = (databaseName: string) =>
      makeMethod(
        "getDatabaseTypeSchema",
        "GET",
        `/v1/databases/${databaseName}/type-schema`,
        Schema.String,
      );

    return {
      authenticate,
      getCurrentUser,
      getDatabases,
      createDatabase,
      createDatabaseScoped,
      deleteDatabase,

      getDatabaseSchema,
      getDatabaseTypeSchema,

      openTransaction,
      closeTransaction,
      commitTransaction,
      rollbackTransaction,
      openTransactionScoped,

      analyze,
      oneShotQuery,
      query,
    };
  });

export type TypeDbDefinition = Effect.Effect.Success<ReturnType<typeof make>>;

export class TypeDb extends Context.Tag("TypeDb")<TypeDb, TypeDbDefinition>() {
  static make = make;
  static layer = (config: TypeDbConfig) =>
    Layer.effect(this, this.make(config));
  static fromFileConfig = Layer.effect(
    this,
    Effect.gen(function* () {
      const cfg = yield* AppFileConfig;
      return yield* make({
        url: cfg.typedb_url,
        username: cfg.typedb_username,
        password: Redacted.value(cfg.typedb_password),
      });
    }),
  );
}
