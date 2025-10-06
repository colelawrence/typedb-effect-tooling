import * as FetchHttpClient from "@effect/platform/FetchHttpClient";
import * as HttpBody from "@effect/platform/HttpBody";
import * as HttpClient from "@effect/platform/HttpClient";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import { NodeRuntime } from "@effect/platform-node";
import * as Schema from "effect/Schema";
import * as HttpIncomingMessage from "@effect/platform/HttpIncomingMessage";
import { pipe } from "effect";
import {
  ApiOkResponse,
  TypeDBHttpDriver,
  isApiErrorResponse,
} from "typedb-driver-http";
import * as Cache from "effect/Cache";
import { decodeJwt } from "jose";
import * as Exit from "effect/Exit";
import * as Duration from "effect/Duration";
import * as HttpClientRequest from "@effect/platform/HttpClientRequest";
import * as Context from "effect/Context";
import * as ParseResult from "effect/ParseResult";

interface TypeDbConfig {
  username: string;
  password: string;
  url: string;
}

// export type ApiOkResponse<OK_RES = {}> = { ok: OK_RES };

// export type ApiError = { code: string; message: string };

// export interface ApiErrorResponse {
//     err: ApiError;
//     status: number;
// }

// export function isApiError(err: any): err is ApiError {
//     return err != null && typeof err.code === "string" && typeof err.message === "string";
// }

// export type ApiResponse<OK_RES = {} | null> = ApiOkResponse<OK_RES> | ApiErrorResponse;
//

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

const ApiOkResponse = <A, I, R>(inner: Schema.Schema<A, I, R>) =>
  Schema.Struct({
    ok: inner,
  });

const ApiResponse = <A, I, R>(inner: Schema.Schema<A, I, R>) =>
  Schema.Union(ApiOkResponse(inner), ApiErrorResponse);

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

const make = ({ username, password, url }: TypeDbConfig) =>
  Effect.gen(function* () {
    const publicHttp = yield* HttpClient.HttpClient;
    const authenticate = Effect.fn("auth")(function* ({
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
        Effect.flatMap(Schema.decodeUnknown(TokenResponse)),
      );

      return tokenRes;
    });

    const http = yield* HttpClient.HttpClient.pipe(
      Effect.map(
        HttpClient.mapRequestEffect((req) =>
          Effect.gen(function* () {
            const token = yield* authenticate({
              username,
              password,
            });
            return HttpClientRequest.bearerToken(token.token)(req);
          }),
        ),
      ),
    );

    const tokenCache = yield* Cache.makeWith({
      capacity: 1,
      lookup: (cfg: TypeDbConfig) => authenticate(cfg),
      timeToLive: Exit.match({
        onFailure: () => Duration.zero,
        onSuccess: (token) => {
          const decoded = Schema.decodeUnknownSync(DecodedToken)(
            decodeJwt(token.token),
          );
          const now = Date.now() / 1000;
          const remaining = Duration.seconds(decoded.exp - now - 2);
          console.log(remaining);
          return remaining;
        },
      }),
    });

    return {
      authenticate,
      getCurrentUser: Effect.fn("getCurrentUser")(function* () {
        return yield* http
          .get(`http://localhost:8000/v1/users/admin`)
          .pipe(Effect.flatMap((r) => r.json));
      }),
    };
  });

export type TypeDbDefinition = Effect.Effect.Success<ReturnType<typeof make>>;

export class TypeDb extends Context.Tag("TypeDb")<TypeDb, TypeDbDefinition>() {
  static make = make;
  static layer = (config: TypeDbConfig) =>
    Layer.effect(this, this.make(config));
}

const Main = TypeDb.layer({
  username: "admin",
  password: "password",
  url: "http://localhost:8000",
}).pipe(Layer.provide(FetchHttpClient.layer));

const main = Effect.gen(function* () {
  const typedb = yield* TypeDb;
  const token = yield* typedb.authenticate({
    username: "admin",
    password: "password",
  });
  yield* Effect.logInfo(token);
  const user = yield* typedb.getCurrentUser();
  yield* Effect.logInfo(user);
}).pipe(Effect.provide(Main));

NodeRuntime.runMain(main);
