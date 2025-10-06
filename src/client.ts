import * as HttpBody from "@effect/platform/HttpBody";
import * as HttpClient from "@effect/platform/HttpClient";
import { ResponseError } from "@effect/platform/HttpClientError";
import * as HttpClientRequest from "@effect/platform/HttpClientRequest";
import * as HttpIncomingMessage from "@effect/platform/HttpIncomingMessage";
import { HttpMethod } from "@effect/platform/HttpMethod";
import * as Cache from "effect/Cache";
import * as Context from "effect/Context";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import { decodeJwt } from "jose";
import {
  QueryOptions,
  TransactionOptions,
  TransactionType,
} from "typedb-driver-http";

export class ApiError extends Schema.Class<ApiError>("ApiError")({
  code: Schema.String,
  message: Schema.String,
}) {}

export class ApiErrorResponse extends Schema.Class<ApiErrorResponse>(
  "ApiErrorResponse",
)({
  err: ApiError,
  status: Schema.Number,
}) {}

const ApiResponse = <A, I, R>(inner: Schema.Schema<A, I, R>) =>
  Schema.Union(inner, ApiError);

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
  url: string;
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
    const publicHttp = yield* HttpClient.HttpClient;
    const authenticate = Effect.fn("TypeDb: signIn")(function* ({
      username,
      password,
    }: Pick<TypeDbConfig, "username" | "password">) {
      const response = yield* publicHttp.post(new URL("/v1/signin", url), {
        body: yield* HttpBody.json({
          username,
          password,
        }),
      });
      const tokenRes = yield* response.json.pipe(
        Effect.flatMap(Schema.decodeUnknown(ApiResponse(TokenResponse))),
      );
      if ("code" in tokenRes) {
        return yield* new TypeDbError({ cause: tokenRes });
      }

      return tokenRes;
    });

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

    const makeMethod = <A, I, R, B extends object>(
      name: string,
      method: HttpMethod,
      path: string,
      schema: Schema.Schema<A, I, R>,
      body?: B,
      getBody: (
        res: HttpIncomingMessage.HttpIncomingMessage<ResponseError>,
      ) => Effect.Effect<unknown, ResponseError> = (res) => res.json,
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
        return yield* http
          .execute(
            HttpClientRequest.make(method)(new URL(path, url), {
              body: b,
            }),
          )
          .pipe(
            Effect.flatMap(getBody),
            Effect.flatMap((x) =>
              Schema.decodeUnknown(schema)(x).pipe(
                Effect.orElse(() =>
                  Schema.decodeUnknown(ApiError)(x).pipe(
                    Effect.flatMap(
                      (e) =>
                        new TypeDbError({
                          cause: e,
                        }),
                    ),
                  ),
                ),
              ),
            ),
            Effect.catchTag("ParseError", Effect.die),
          );
      }).pipe(Effect.withSpan(`TypeDb: ${name}`));

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
        Schema.Null,
      );

    const commitTransaction = (transactionId: string) =>
      makeMethod(
        "commitTransaction",
        "POST",
        `/v1/transactions/${transactionId}/commit`,
        Schema.Null,
      );

    const rollbackTransaction = (transactionId: string) =>
      makeMethod(
        "rollbackTransaction",
        "POST",
        `/v1/transactions/${transactionId}/rollback`,
        Schema.Null,
      );

    const openTransactionScoped = (dbName: string, txType: TransactionType) =>
      Effect.acquireRelease(openTransaction(dbName, txType), (tx, exit) =>
        Exit.match(exit, {
          onFailure: () =>
            closeTransaction(tx.transactionId).pipe(Effect.orDie),
          onSuccess: () =>
            commitTransaction(tx.transactionId).pipe(Effect.orDie),
        }),
      ).pipe(
        Effect.annotateSpans({
          txType,
        }),
      );

    const analyze = (transactionId: string, query: string) =>
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
        Schema.Null,
      );

    const deleteDatabase = (databaseName: string) =>
      makeMethod(
        "deleteDatabase",
        "DELETE",
        `/v1/databases/${databaseName}`,
        Schema.Null,
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
        undefined,
        (res) => res.text,
      );
    const getDatabaseTypeSchema = (databaseName: string) =>
      makeMethod(
        "getDatabaseTypeSchema",
        "GET",
        `/v1/databases/${databaseName}/type-schema`,
        Schema.String,
        undefined,
        (res) => res.text,
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
}
