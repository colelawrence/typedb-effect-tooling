import * as HttpBody from "@effect/platform/HttpBody";
import * as HttpClient from "@effect/platform/HttpClient";
import * as HttpClientRequest from "@effect/platform/HttpClientRequest";
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
import { TransactionType } from "typedb-driver-http";

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

const make = ({ username, password, url }: TypeDbConfig) =>
  Effect.gen(function* () {
    const publicHttp = yield* HttpClient.HttpClient;

    const authenticate = Effect.fn("TypeDb /v1/signin")(function* ({
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

    const http = yield* HttpClient.HttpClient.pipe(
      Effect.map(
        HttpClient.mapRequestEffect((req) =>
          tokenCache
            .get({ username, password })
            .pipe(
              Effect.map((token) =>
                req.pipe(HttpClientRequest.bearerToken(token.token)),
              ),
            ),
        ),
      ),
    );

    const makeMethod = <A, I, R, B extends object>(
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
        return yield* http
          .execute(
            HttpClientRequest.make(method)(new URL(path, url), {
              body: b,
            }),
          )
          .pipe(
            Effect.flatMap((_) => _.json),
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
            Effect.withSpan(`TypeDb ${path}`),
          );
      });

    const closeTransaction = (transactionId: string) =>
      makeMethod(
        "POST",
        `/v1/transactions/${transactionId}/close`,
        Schema.Null,
      );

    const commitTransaction = (transactionId: string) =>
      makeMethod(
        "POST",
        `/v1/transactions/${transactionId}/commit`,
        Schema.Struct({}),
      );

    const rollbackTransaction = (transactionId: string) =>
      makeMethod(
        "POST",
        `/v1/transactions/${transactionId}/rollback`,
        Schema.Struct({}),
      );

    const analyze = (transactionId: string, query: string) =>
      makeMethod(
        "POST",
        `/v1/transactions/${transactionId}/analyze`,
        Schema.Any,
        {
          query,
        },
      );

    const openTransaction = (
      databaseName: string,
      transactionType: TransactionType,
    ) =>
      makeMethod(
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

    const getCurrentUser = makeMethod(
      "GET",
      `/v1/users/${username}`,
      Schema.Struct({
        username: Schema.String,
      }),
    );
    const getDatabases = makeMethod(
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
    return {
      authenticate,
      getCurrentUser,
      getDatabases,
      openTransaction,
      closeTransaction,
      commitTransaction,
      rollbackTransaction,
      analyze,
    };
  });

export type TypeDbDefinition = Effect.Effect.Success<ReturnType<typeof make>>;

export class TypeDb extends Context.Tag("TypeDb")<TypeDb, TypeDbDefinition>() {
  static make = make;
  static layer = (config: TypeDbConfig) =>
    Layer.effect(this, this.make(config));
}
