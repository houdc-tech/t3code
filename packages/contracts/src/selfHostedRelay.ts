import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

import { TrimmedString } from "./baseSchemas.ts";
import { SettingsSecretValue } from "./settingsSecretValue.ts";

export const SELF_HOSTED_RELAY_PROVIDER_ID = "self-hosted-relay" as const;

const SelfHostedRelayCredential = SettingsSecretValue.pipe(
  Schema.withDecodingDefault(Effect.succeed("")),
  Schema.annotateKey({ settingsSecret: true }),
);

export const SelfHostedRelaySettings = Schema.Struct({
  enabled: Schema.Boolean.pipe(Schema.withDecodingDefault(Effect.succeed(false))),
  serverUrl: TrimmedString.pipe(Schema.withDecodingDefault(Effect.succeed(""))),
  deviceName: TrimmedString.pipe(Schema.withDecodingDefault(Effect.succeed(""))),
  authCredential: SelfHostedRelayCredential,
  autoConnect: Schema.Boolean.pipe(Schema.withDecodingDefault(Effect.succeed(true))),
});
export type SelfHostedRelaySettings = typeof SelfHostedRelaySettings.Type;

export const DEFAULT_SELF_HOSTED_RELAY_SETTINGS: SelfHostedRelaySettings = Schema.decodeSync(
  SelfHostedRelaySettings,
)({});

export const SelfHostedRelaySettingsPatch = Schema.Struct({
  enabled: Schema.optionalKey(Schema.Boolean),
  serverUrl: Schema.optionalKey(TrimmedString),
  deviceName: Schema.optionalKey(TrimmedString),
  authCredential: Schema.optionalKey(SettingsSecretValue),
  autoConnect: Schema.optionalKey(Schema.Boolean),
});
export type SelfHostedRelaySettingsPatch = typeof SelfHostedRelaySettingsPatch.Type;

export const SelfHostedRelayPhase = Schema.Literals([
  "disabled",
  "disconnected",
  "connecting",
  "connected",
  "error",
]);
export type SelfHostedRelayPhase = typeof SelfHostedRelayPhase.Type;

export const SelfHostedRelayStatus = Schema.Struct({
  phase: SelfHostedRelayPhase,
  publicHttpBaseUrl: Schema.optionalKey(TrimmedString),
  publicWsBaseUrl: Schema.optionalKey(TrimmedString),
  connectedAt: Schema.optionalKey(TrimmedString),
  lastError: Schema.optionalKey(TrimmedString),
  activeStreams: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)).pipe(
    Schema.withDecodingDefault(Effect.succeed(0)),
  ),
});
export type SelfHostedRelayStatus = typeof SelfHostedRelayStatus.Type;

export const SelfHostedRelayActionResult = Schema.Struct({
  status: SelfHostedRelayStatus,
});
export type SelfHostedRelayActionResult = typeof SelfHostedRelayActionResult.Type;
