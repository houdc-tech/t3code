import {
  type AdvertisedEndpoint,
  SELF_HOSTED_RELAY_PROVIDER_ID,
  type SelfHostedRelayStatus,
} from "@t3tools/contracts";

const SELF_HOSTED_RELAY_ROUTE_LABEL = "Self-hosted Relay";

export function advertisedSelfHostedRelayEndpoint(
  status: SelfHostedRelayStatus | null | undefined,
): AdvertisedEndpoint | null {
  if (
    status === null ||
    status === undefined ||
    status.phase !== "connected" ||
    !status.publicHttpBaseUrl ||
    !status.publicWsBaseUrl
  ) {
    return null;
  }
  const isHttps = status.publicHttpBaseUrl.startsWith("https:");
  return {
    id: SELF_HOSTED_RELAY_PROVIDER_ID,
    label: SELF_HOSTED_RELAY_ROUTE_LABEL,
    provider: {
      id: SELF_HOSTED_RELAY_PROVIDER_ID,
      label: SELF_HOSTED_RELAY_ROUTE_LABEL,
      kind: "tunnel",
      isAddon: false,
    },
    httpBaseUrl: status.publicHttpBaseUrl,
    wsBaseUrl: status.publicWsBaseUrl,
    reachability: "public",
    compatibility: {
      hostedHttpsApp: isHttps ? "compatible" : "mixed-content-blocked",
      desktopApp: "compatible",
    },
    source: "server",
    status: "available",
  };
}
