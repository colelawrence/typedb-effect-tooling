import { NodeSdk } from "@effect/opentelemetry";
import { OTLPTraceExporter } from "@opentelemetry/exporter-trace-otlp-http";
import { BatchSpanProcessor } from "@opentelemetry/sdk-trace-node";

export const TelemetryLive = NodeSdk.layer(() => ({
  resource: { serviceName: "typedb-client" },
  spanProcessor: new BatchSpanProcessor(new OTLPTraceExporter()),
}));
