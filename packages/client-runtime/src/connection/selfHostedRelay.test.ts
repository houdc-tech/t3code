import { EnvironmentId } from "@t3tools/contracts";
import * as Option from "effect/Option";
import { describe, expect, it } from "vite-plus/test";

import { BearerConnectionProfile, type ConnectionRoute } from "./catalog.ts";
import { BearerConnectionTarget } from "./model.ts";
import { connectionRouteKind, connectionRouteLabel, insertRoute } from "./routes.ts";
import { advertisedSelfHostedRelayEndpoint } from "./selfHostedRelay.ts";

const envId = EnvironmentId.make("env_test");

function makeRoute(
  connectionId: string,
  httpBaseUrl: string,
  relayProvider?: "self-hosted-relay",
): ConnectionRoute {
  return {
    target: new BearerConnectionTarget({
      environmentId: envId,
      label: "Mac Studio",
      connectionId,
    }),
    profile: Option.some(
      new BearerConnectionProfile({
        connectionId,
        environmentId: envId,
        label: "Mac Studio",
        httpBaseUrl,
        wsBaseUrl: httpBaseUrl.replace(/^http/, "ws"),
        ...(relayProvider ? { relayProvider } : {}),
      }),
    ),
  };
}

describe("self-hosted relay routes", () => {
  it("classifies and orders self-hosted-relay behind LAN and public routes", () => {
    const relayRoute = makeRoute(
      "self-relay",
      "https://studio.relay.example.com/",
      "self-hosted-relay",
    );
    const lanRoute = makeRoute("lan", "http://192.168.1.50:3773/");

    expect(connectionRouteKind(relayRoute)).toBe("self-hosted-relay");
    expect(connectionRouteLabel(relayRoute)).toBe("Self-hosted Relay");

    const ordered = insertRoute([relayRoute], lanRoute);
    expect(ordered.map((r) => connectionRouteKind(r))).toEqual(["lan", "self-hosted-relay"]);
  });

  it("builds an advertised endpoint only when connected with public URLs", () => {
    expect(
      advertisedSelfHostedRelayEndpoint({ phase: "disconnected", activeStreams: 0 }),
    ).toBeNull();
    const endpoint = advertisedSelfHostedRelayEndpoint({
      phase: "connected",
      publicHttpBaseUrl: "https://studio.relay.example.com",
      publicWsBaseUrl: "wss://studio.relay.example.com",
      activeStreams: 2,
    });
    expect(endpoint?.provider.id).toBe("self-hosted-relay");
    expect(endpoint?.compatibility.hostedHttpsApp).toBe("compatible");
  });
});
