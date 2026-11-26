import { describe, expect, it } from "vite-plus/test";
import {
  buildControlWebSocketUrl,
  buildStreamWebSocketUrl,
  parseRelayControlServerMessage,
  sanitizeRelayErrorMessage,
  validateRegisteredPublicUrls,
} from "./protocol.ts";

describe("selfHostedRelay protocol", () => {
  it("normalizes control and stream WebSocket URLs", () => {
    expect(buildControlWebSocketUrl("https://mac.relay.example.com")).toBe(
      "wss://mac.relay.example.com/_relay/control",
    );
    expect(buildControlWebSocketUrl("mac.relay.example.com:8443/ignored?x=1")).toBe(
      "wss://mac.relay.example.com:8443/_relay/control",
    );
    expect(buildControlWebSocketUrl("http://127.0.0.1:8080")).toBe(
      "ws://127.0.0.1:8080/_relay/control",
    );
    expect(buildStreamWebSocketUrl("https://mac.relay.example.com", "str_123")).toBe(
      "wss://mac.relay.example.com/_relay/stream/str_123",
    );
  });

  it("parses valid server control messages and ignores invalid ones", () => {
    expect(
      parseRelayControlServerMessage(
        JSON.stringify({
          type: "registered",
          protocolVersion: 1,
          publicHttpBaseUrl: "https://mac.relay.example.com",
          publicWsBaseUrl: "wss://mac.relay.example.com",
          heartbeatIntervalSec: 25,
        }),
      ),
    ).toEqual({
      type: "registered",
      protocolVersion: 1,
      publicHttpBaseUrl: "https://mac.relay.example.com",
      publicWsBaseUrl: "wss://mac.relay.example.com",
      heartbeatIntervalSec: 25,
    });

    expect(
      parseRelayControlServerMessage(
        JSON.stringify({
          type: "open_stream",
          streamId: "str_1",
          streamToken: "tok_1",
          expiresAtMs: 12345,
        }),
      ),
    ).toEqual({
      type: "open_stream",
      streamId: "str_1",
      streamToken: "tok_1",
      expiresAtMs: 12345,
    });

    expect(parseRelayControlServerMessage("not-json")).toBeNull();
    expect(parseRelayControlServerMessage(JSON.stringify({ type: "unknown" }))).toBeNull();
  });

  it("redacts bearer tokens and query secrets from error messages", () => {
    expect(
      sanitizeRelayErrorMessage(new Error("Auth failed: Bearer super-secret-token token=abc123")),
    ).toBe("Auth failed: Bearer [redacted] token=[redacted]");
  });

  it("validates registered public URLs against the configured serverUrl origin", () => {
    expect(
      validateRegisteredPublicUrls(
        "https://mac.relay.example.com",
        "https://mac.relay.example.com",
        "wss://mac.relay.example.com",
      ),
    ).toEqual({
      publicHttpBaseUrl: "https://mac.relay.example.com",
      publicWsBaseUrl: "wss://mac.relay.example.com",
    });

    expect(
      validateRegisteredPublicUrls(
        "https://mac.relay.example.com",
        "https://evil.example.com",
        "wss://evil.example.com",
      ),
    ).toBeNull();

    expect(
      validateRegisteredPublicUrls("https://mac.relay.example.com", "not-a-url", "wss://x"),
    ).toBeNull();
  });
});
