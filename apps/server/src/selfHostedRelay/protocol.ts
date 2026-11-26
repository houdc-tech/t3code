export const SELF_HOSTED_RELAY_PROTOCOL_VERSION = 1;

export type RelayControlClientMessage =
  | {
      readonly type: "register";
      readonly protocolVersion: number;
      readonly deviceName: string;
      readonly environmentId: string;
    }
  | {
      readonly type: "stream_rejected";
      readonly streamId: string;
      readonly reason: string;
    }
  | {
      readonly type: "ping";
      readonly ts: number;
    }
  | {
      readonly type: "pong";
      readonly ts: number;
    }
  | {
      readonly type: "disconnect";
      readonly reason: string;
    };

export type RelayControlServerMessage =
  | {
      readonly type: "registered";
      readonly protocolVersion: number;
      readonly publicHttpBaseUrl: string;
      readonly publicWsBaseUrl: string;
      readonly heartbeatIntervalSec: number;
    }
  | {
      readonly type: "open_stream";
      readonly streamId: string;
      readonly streamToken: string;
      readonly expiresAtMs: number;
    }
  | {
      readonly type: "ping";
      readonly ts: number;
    }
  | {
      readonly type: "pong";
      readonly ts: number;
    }
  | {
      readonly type: "error";
      readonly message: string;
    };

function normalizeRelayBaseUrl(serverUrl: string): URL {
  const trimmed = serverUrl.trim();
  if (!trimmed) {
    throw new Error("Relay server URL is required.");
  }
  const withScheme = /^[a-z][a-z0-9+.-]*:\/\//i.test(trimmed) ? trimmed : `https://${trimmed}`;
  const url = new URL(withScheme);
  if (
    url.protocol !== "https:" &&
    url.protocol !== "http:" &&
    url.protocol !== "wss:" &&
    url.protocol !== "ws:"
  ) {
    throw new Error("Relay server URL must use https://, wss://, http://, or ws://.");
  }
  url.protocol = url.protocol === "http:" || url.protocol === "ws:" ? "ws:" : "wss:";
  url.search = "";
  url.hash = "";
  return url;
}

export function buildControlWebSocketUrl(serverUrl: string): string {
  const url = normalizeRelayBaseUrl(serverUrl);
  url.pathname = "/_relay/control";
  return url.toString();
}

export function buildStreamWebSocketUrl(serverUrl: string, streamId: string): string {
  const url = normalizeRelayBaseUrl(serverUrl);
  url.pathname = `/_relay/stream/${encodeURIComponent(streamId)}`;
  return url.toString();
}

export function validateRegisteredPublicUrls(
  serverUrl: string,
  publicHttpBaseUrl: string,
  publicWsBaseUrl: string,
): { readonly publicHttpBaseUrl: string; readonly publicWsBaseUrl: string } | null {
  try {
    const expected = normalizeRelayBaseUrl(serverUrl);
    const httpUrl = new URL(publicHttpBaseUrl);
    const wsUrl = new URL(publicWsBaseUrl);
    if (httpUrl.protocol !== "https:" && httpUrl.protocol !== "http:") return null;
    if (wsUrl.protocol !== "wss:" && wsUrl.protocol !== "ws:") return null;
    if (httpUrl.host.toLowerCase() !== expected.host.toLowerCase()) return null;
    if (wsUrl.host.toLowerCase() !== expected.host.toLowerCase()) return null;
    return {
      publicHttpBaseUrl: `${httpUrl.protocol}//${httpUrl.host}`,
      publicWsBaseUrl: `${wsUrl.protocol}//${wsUrl.host}`,
    };
  } catch {
    return null;
  }
}

export function parseRelayControlServerMessage(raw: string): RelayControlServerMessage | null {
  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch {
    return null;
  }
  if (typeof parsed !== "object" || parsed === null || !("type" in parsed)) {
    return null;
  }
  const record = parsed as Record<string, unknown>;
  switch (record.type) {
    case "registered":
      if (
        typeof record.publicHttpBaseUrl === "string" &&
        typeof record.publicWsBaseUrl === "string"
      ) {
        return {
          type: "registered",
          protocolVersion:
            typeof record.protocolVersion === "number"
              ? record.protocolVersion
              : SELF_HOSTED_RELAY_PROTOCOL_VERSION,
          publicHttpBaseUrl: record.publicHttpBaseUrl,
          publicWsBaseUrl: record.publicWsBaseUrl,
          heartbeatIntervalSec:
            typeof record.heartbeatIntervalSec === "number" && record.heartbeatIntervalSec > 0
              ? record.heartbeatIntervalSec
              : 25,
        };
      }
      return null;
    case "open_stream":
      if (typeof record.streamId === "string" && typeof record.streamToken === "string") {
        return {
          type: "open_stream",
          streamId: record.streamId,
          streamToken: record.streamToken,
          expiresAtMs: typeof record.expiresAtMs === "number" ? record.expiresAtMs : 0,
        };
      }
      return null;
    case "ping":
      return { type: "ping", ts: typeof record.ts === "number" ? record.ts : 0 };
    case "pong":
      return { type: "pong", ts: typeof record.ts === "number" ? record.ts : 0 };
    case "error":
      return {
        type: "error",
        message: typeof record.message === "string" ? record.message : "Relay error.",
      };
    default:
      return null;
  }
}

export function sanitizeRelayErrorMessage(error: unknown): string {
  const raw = error instanceof Error ? error.message : String(error);
  return raw
    .replace(/Bearer\s+\S+/gi, "Bearer [redacted]")
    .replace(/(token|credential|secret)=[^&\s]+/gi, "$1=[redacted]")
    .slice(0, 240);
}
