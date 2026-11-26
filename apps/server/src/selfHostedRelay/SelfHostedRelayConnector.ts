import * as NodeModule from "node:module";
import * as NodeNet from "node:net";
import type * as Undici from "undici";
import {
  isSecretReference,
  type SelfHostedRelaySettings,
  type SelfHostedRelayStatus,
} from "@t3tools/contracts";
import * as Clock from "effect/Clock";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Random from "effect/Random";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import * as SubscriptionRef from "effect/SubscriptionRef";
import * as HttpServer from "effect/http/HttpServer";

import * as ServerEnvironment from "../environment/ServerEnvironment.ts";
import * as ManagedLimits from "../policy/managedLimits.ts";
import * as ManagedPolicy from "../policy/ManagedPolicy.ts";
import * as ServerSettings from "../serverSettings.ts";
import { loopbackUrlHostForAddress } from "../startupAccess.ts";
import {
  buildControlWebSocketUrl,
  buildStreamWebSocketUrl,
  parseRelayControlServerMessage,
  sanitizeRelayErrorMessage,
  SELF_HOSTED_RELAY_PROTOCOL_VERSION,
  validateRegisteredPublicUrls,
  type RelayControlClientMessage,
} from "./protocol.ts";

// undici is external to the CLI bundle; Node single-executables load it via createRequire.
const requireUndici = NodeModule.createRequire(import.meta.url);

function loadUndiciWebSocket(): typeof Undici.WebSocket {
  const undici = requireUndici("undici") as typeof Undici;
  return undici.WebSocket;
}

const RECONNECT_BASE_DELAY_MS = 1_000;
const RECONNECT_MAX_DELAY_MS = 60_000;
const STABLE_CONNECTION_RESET_MS = 30_000;
const MAX_CONCURRENT_STREAMS = 64;
const STREAM_CONNECT_TIMEOUT_MS = 10_000;
const WS_HIGH_WATER_MARK_BYTES = 256 * 1024;
const WS_DRAIN_POLL_MS = 15;

const DISABLED_STATUS: SelfHostedRelayStatus = {
  phase: "disabled",
  activeStreams: 0,
};

const DISCONNECTED_STATUS: SelfHostedRelayStatus = {
  phase: "disconnected",
  activeStreams: 0,
};

class RelaySessionError extends Schema.TaggedError<RelaySessionError>()("RelaySessionError", {
  message: Schema.String,
}) {}

export class SelfHostedRelayConnector extends Context.Service<
  SelfHostedRelayConnector,
  {
    readonly getStatus: Effect.Effect<SelfHostedRelayStatus>;
    readonly subscribeStatus: Stream.Stream<SelfHostedRelayStatus>;
    readonly connect: Effect.Effect<SelfHostedRelayStatus>;
    readonly disconnect: Effect.Effect<SelfHostedRelayStatus>;
  }
>()("t3/selfHostedRelay/SelfHostedRelayConnector") {}

interface ActiveConnectionConfig {
  readonly serverUrl: string;
  readonly deviceName: string;
  readonly authToken: string;
  readonly environmentId: string;
  readonly localHost: string;
  readonly localPort: number;
}

function resolvePlainCredential(settings: SelfHostedRelaySettings): string {
  return typeof settings.authCredential === "string" ? settings.authCredential.trim() : "";
}

function configSignature(settings: SelfHostedRelaySettings): string {
  return JSON.stringify({
    enabled: settings.enabled,
    serverUrl: settings.serverUrl.trim(),
    deviceName: settings.deviceName.trim(),
    authCredential: isSecretReference(settings.authCredential)
      ? settings.authCredential
      : settings.authCredential,
    autoConnect: settings.autoConnect,
  });
}

function resolveLocalDialTarget(
  address: HttpServer.HttpServer["Service"]["address"],
): { readonly host: string; readonly port: number } | null {
  if (address._tag === "UnixPathAddress") {
    return null;
  }
  const host = loopbackUrlHostForAddress(address).replace(/^\[|\]$/g, "");
  return { host, port: address.port };
}

function openBinaryDataStream(input: {
  readonly WebSocketCtor: typeof Undici.WebSocket;
  readonly runFork: <A, E>(effect: Effect.Effect<A, E>) => Fiber.Fiber<A, E>;
  readonly serverUrl: string;
  readonly streamId: string;
  readonly streamToken: string;
  readonly localHost: string;
  readonly localPort: number;
  readonly onClose: () => void;
}): () => void {
  let closed = false;
  let ws: Undici.WebSocket | null = null;
  let tcp: NodeNet.Socket | null = null;
  let drainFiber: Fiber.Fiber<void, never> | null = null;
  const pendingToTcp: Array<Uint8Array> = [];

  const stopFiber = (fiber: Fiber.Fiber<void, never> | null) => {
    if (fiber !== null) {
      input.runFork(Fiber.interrupt(fiber));
    }
  };

  let connectFiber: Fiber.Fiber<void, never> | null = null;

  const cleanup = () => {
    if (closed) return;
    closed = true;
    stopFiber(connectFiber);
    connectFiber = null;
    stopFiber(drainFiber);
    drainFiber = null;
    if (tcp) {
      tcp.destroy();
      tcp = null;
    }
    if (ws) {
      try {
        if (
          ws.readyState === input.WebSocketCtor.OPEN ||
          ws.readyState === input.WebSocketCtor.CONNECTING
        ) {
          ws.close(1000, "stream closed");
        }
      } catch {
        // Ignore close errors during teardown.
      }
      ws = null;
    }
    input.onClose();
  };

  connectFiber = input.runFork(
    Effect.sleep(Duration.millis(STREAM_CONNECT_TIMEOUT_MS)).pipe(
      Effect.andThen(Effect.sync(cleanup)),
    ),
  );

  try {
    const streamUrl = buildStreamWebSocketUrl(input.serverUrl, input.streamId);
    ws = new input.WebSocketCtor(streamUrl, {
      headers: {
        authorization: `Bearer ${input.streamToken}`,
      },
    });
    ws.binaryType = "arraybuffer";
  } catch {
    cleanup();
    return cleanup;
  }

  ws.addEventListener("open", () => {
    if (closed) return;
    stopFiber(connectFiber);
    connectFiber = null;

    const socket = NodeNet.createConnection({
      host: input.localHost,
      port: input.localPort,
    });
    tcp = socket;
    socket.setNoDelay(true);

    socket.on("connect", () => {
      if (closed) {
        socket.destroy();
        return;
      }
      while (pendingToTcp.length > 0) {
        const chunk = pendingToTcp.shift()!;
        socket.write(chunk);
      }
    });

    socket.on("data", (chunk: Buffer) => {
      if (closed || !ws || ws.readyState !== input.WebSocketCtor.OPEN) {
        cleanup();
        return;
      }
      try {
        ws.send(chunk);
        if (ws.bufferedAmount > WS_HIGH_WATER_MARK_BYTES && !socket.isPaused()) {
          socket.pause();
          if (drainFiber === null) {
            drainFiber = input.runFork(
              Effect.sleep(Duration.millis(WS_DRAIN_POLL_MS)).pipe(
                Effect.repeat({
                  while: () =>
                    !closed &&
                    ws !== null &&
                    tcp !== null &&
                    ws.bufferedAmount > WS_HIGH_WATER_MARK_BYTES / 2,
                }),
                Effect.andThen(
                  Effect.sync(() => {
                    drainFiber = null;
                    if (!closed && tcp) {
                      tcp.resume();
                    }
                  }),
                ),
              ),
            );
          }
        }
      } catch {
        cleanup();
      }
    });

    socket.on("end", () => cleanup());
    socket.on("close", () => cleanup());
    socket.on("error", () => cleanup());
  });

  ws.addEventListener("message", (event) => {
    if (closed) return;
    const data = event.data;
    let bytes: Uint8Array;
    if (data instanceof ArrayBuffer) {
      bytes = new Uint8Array(data);
    } else if (ArrayBuffer.isView(data)) {
      bytes = new Uint8Array(data.buffer, data.byteOffset, data.byteLength);
    } else if (typeof data === "string") {
      bytes = new TextEncoder().encode(data);
    } else {
      return;
    }
    if (tcp && !tcp.pending && tcp.writable) {
      tcp.write(bytes);
    } else {
      pendingToTcp.push(bytes);
    }
  });

  ws.addEventListener("close", () => cleanup());
  ws.addEventListener("error", () => cleanup());

  return cleanup;
}

const make = Effect.gen(function* () {
  const serverSettings = yield* ServerSettings.ServerSettingsService;
  const environment = yield* ServerEnvironment.ServerEnvironment;
  const managedPolicy = yield* ManagedPolicy.ManagedPolicy;
  const httpServer = yield* HttpServer.HttpServer;
  const serviceScope = yield* Scope.Scope;

  const statusRef = yield* SubscriptionRef.make<SelfHostedRelayStatus>(DISABLED_STATUS);
  const loopFiberRef = yield* Ref.make<Fiber.Fiber<void, never> | null>(null);
  const manualConnectRef = yield* Ref.make(false);

  const localTarget = resolveLocalDialTarget(httpServer.address);
  const environmentId = yield* environment.getEnvironmentId;

  const stopLoop = Effect.gen(function* () {
    const current = yield* Ref.getAndSet(loopFiberRef, null);
    if (current !== null) {
      yield* Fiber.interrupt(current);
    }
  });

  const runSessionOnce = Effect.fn("SelfHostedRelayConnector.runSessionOnce")(function* (
    config: ActiveConnectionConfig,
  ) {
    const services = yield* Effect.context<never>();
    const runFork = Effect.runForkWith(services);
    const clock = yield* Clock.Clock;

    return yield* Effect.callback<void, RelaySessionError>((resume) => {
      let settled = false;
      let heartbeatFiber: Fiber.Fiber<void, never> | null = null;
      let lastPongAt = clock.currentTimeMillisUnsafe();
      const activeClosers = new Set<() => void>();
      let currentStatus: SelfHostedRelayStatus = {
        phase: "connecting",
        activeStreams: 0,
      };

      const publishStatus = (next: SelfHostedRelayStatus) => {
        currentStatus = next;
        runFork(SubscriptionRef.set(statusRef, next));
      };

      const closeAllStreams = () => {
        for (const closeStream of Array.from(activeClosers)) {
          closeStream();
        }
        activeClosers.clear();
      };

      let WebSocketCtor: typeof Undici.WebSocket;
      try {
        WebSocketCtor = loadUndiciWebSocket();
      } catch (error) {
        resume(Effect.fail(new RelaySessionError({ message: sanitizeRelayErrorMessage(error) })));
        return Effect.void;
      }

      let ws: Undici.WebSocket | null = null;
      const sendControl = (msg: RelayControlClientMessage) => {
        if (ws && ws.readyState === WebSocketCtor.OPEN) {
          ws.send(JSON.stringify(msg));
        }
      };

      const finish = (message?: string) => {
        if (settled) return;
        settled = true;
        if (heartbeatFiber !== null) {
          runFork(Fiber.interrupt(heartbeatFiber));
          heartbeatFiber = null;
        }
        closeAllStreams();
        if (ws) {
          try {
            if (ws.readyState === WebSocketCtor.OPEN) {
              sendControl({ type: "disconnect", reason: "closing" });
              ws.close(1000, "closing");
            } else if (ws.readyState === WebSocketCtor.CONNECTING) {
              ws.close();
            }
          } catch {
            // Ignore close errors.
          }
          ws = null;
        }
        resume(message ? Effect.fail(new RelaySessionError({ message })) : Effect.void);
      };

      publishStatus({ phase: "connecting", activeStreams: 0 });

      let controlUrl: string;
      try {
        controlUrl = buildControlWebSocketUrl(config.serverUrl);
      } catch (error) {
        finish(sanitizeRelayErrorMessage(error));
        return Effect.void;
      }

      try {
        ws = new WebSocketCtor(controlUrl, {
          headers: {
            authorization: `Bearer ${config.authToken}`,
          },
        });
      } catch (error) {
        finish(sanitizeRelayErrorMessage(error));
        return Effect.void;
      }

      ws.addEventListener("open", () => {
        lastPongAt = clock.currentTimeMillisUnsafe();
        sendControl({
          type: "register",
          protocolVersion: SELF_HOSTED_RELAY_PROTOCOL_VERSION,
          deviceName: config.deviceName,
          environmentId: config.environmentId,
        });
      });

      ws.addEventListener("message", (event) => {
        const raw = typeof event.data === "string" ? event.data : "";
        const message = parseRelayControlServerMessage(raw);
        if (message === null) return;

        switch (message.type) {
          case "registered": {
            const validated = validateRegisteredPublicUrls(
              config.serverUrl,
              message.publicHttpBaseUrl,
              message.publicWsBaseUrl,
            );
            if (validated === null) {
              finish("Relay returned a public URL that does not match serverUrl.");
              return;
            }
            const nowMs = clock.currentTimeMillisUnsafe();
            lastPongAt = nowMs;
            publishStatus({
              phase: "connected",
              publicHttpBaseUrl: validated.publicHttpBaseUrl,
              publicWsBaseUrl: validated.publicWsBaseUrl,
              connectedAt: DateTime.formatIso(DateTime.makeUnsafe(nowMs)),
              activeStreams: activeClosers.size,
            });
            if (heartbeatFiber !== null) {
              runFork(Fiber.interrupt(heartbeatFiber));
            }
            const intervalMs = Math.max(5_000, message.heartbeatIntervalSec * 1_000);
            heartbeatFiber = runFork(
              Effect.sleep(Duration.millis(intervalMs)).pipe(
                Effect.andThen(
                  Effect.sync(() => {
                    const currentMs = clock.currentTimeMillisUnsafe();
                    if (currentMs - lastPongAt > intervalMs * 2 + 5_000) {
                      finish("Relay control heartbeat timed out.");
                      return;
                    }
                    sendControl({ type: "ping", ts: currentMs });
                  }),
                ),
                Effect.forever,
              ),
            );
            break;
          }
          case "open_stream": {
            if (activeClosers.size >= MAX_CONCURRENT_STREAMS) {
              sendControl({
                type: "stream_rejected",
                streamId: message.streamId,
                reason: "capacity_exceeded",
              });
              return;
            }
            // Track pending streams immediately so capacity and disconnect cleanup cover handshakes.
            let closeStream: () => void = () => {};
            let removed = false;
            const release = () => {
              if (removed) return;
              removed = true;
              activeClosers.delete(closeStream);
              if (currentStatus.phase === "connected") {
                publishStatus({ ...currentStatus, activeStreams: activeClosers.size });
              }
            };
            closeStream = openBinaryDataStream({
              WebSocketCtor,
              runFork,
              serverUrl: config.serverUrl,
              streamId: message.streamId,
              streamToken: message.streamToken,
              localHost: config.localHost,
              localPort: config.localPort,
              onClose: release,
            });
            if (!removed) {
              activeClosers.add(closeStream);
              if (currentStatus.phase === "connected") {
                publishStatus({ ...currentStatus, activeStreams: activeClosers.size });
              }
            }
            break;
          }
          case "ping": {
            lastPongAt = clock.currentTimeMillisUnsafe();
            sendControl({ type: "pong", ts: message.ts });
            break;
          }
          case "pong": {
            lastPongAt = clock.currentTimeMillisUnsafe();
            break;
          }
          case "error": {
            finish(message.message);
            break;
          }
        }
      });

      ws.addEventListener("close", (event) => {
        finish(
          event.reason
            ? `Relay connection closed (${event.code}: ${event.reason}).`
            : `Relay connection closed (${event.code}).`,
        );
      });

      ws.addEventListener("error", () => {
        finish("Relay control WebSocket error.");
      });

      return Effect.sync(() => {
        finish();
      });
    });
  });

  const startLoop = Effect.fn("SelfHostedRelayConnector.startLoop")(function* (
    config: ActiveConnectionConfig,
  ) {
    yield* stopLoop;
    const fiber = yield* Effect.gen(function* () {
      let failureCount = 0;
      for (;;) {
        const startedAt = yield* Clock.currentTimeMillis;
        const result = yield* Effect.result(runSessionOnce(config));
        const uptimeMs = (yield* Clock.currentTimeMillis) - startedAt;
        if (uptimeMs >= STABLE_CONNECTION_RESET_MS) {
          failureCount = 0;
        } else {
          failureCount += 1;
        }
        const errorMessage =
          result._tag === "Failure"
            ? sanitizeRelayErrorMessage(result.failure.message)
            : "Relay connection closed.";
        yield* SubscriptionRef.set(statusRef, {
          phase: "error",
          lastError: errorMessage,
          activeStreams: 0,
        });
        const ceiling = Math.min(
          RECONNECT_MAX_DELAY_MS,
          RECONNECT_BASE_DELAY_MS * 2 ** Math.min(failureCount, 6),
        );
        const jitter = yield* Random.next;
        const delayMs = Math.round(ceiling / 2 + (ceiling / 2) * jitter);
        yield* Effect.sleep(Duration.millis(delayMs));
      }
    }).pipe(Effect.forkIn(serviceScope));
    yield* Ref.set(loopFiberRef, fiber);
  });

  const reconcile = Effect.fn("SelfHostedRelayConnector.reconcile")(function* (
    settings: SelfHostedRelaySettings,
    forceConnect: boolean,
  ) {
    if (!ManagedLimits.allowsRemoteClients(managedPolicy) || !settings.enabled) {
      yield* Ref.set(manualConnectRef, false);
      yield* stopLoop;
      yield* SubscriptionRef.set(statusRef, DISABLED_STATUS);
      return;
    }

    const shouldConnect =
      forceConnect || settings.autoConnect || (yield* Ref.get(manualConnectRef));
    if (!shouldConnect) {
      yield* stopLoop;
      yield* SubscriptionRef.set(statusRef, DISCONNECTED_STATUS);
      return;
    }

    const serverUrl = settings.serverUrl.trim();
    const authToken = resolvePlainCredential(settings);
    if (!serverUrl || !authToken) {
      yield* stopLoop;
      yield* SubscriptionRef.set(statusRef, {
        phase: "error",
        lastError: !serverUrl
          ? "Relay server URL is not configured."
          : "Relay authentication credential is not configured.",
        activeStreams: 0,
      });
      return;
    }

    if (localTarget === null) {
      yield* stopLoop;
      yield* SubscriptionRef.set(statusRef, {
        phase: "error",
        lastError: "Local HTTP server address is unavailable.",
        activeStreams: 0,
      });
      return;
    }

    const deviceName = settings.deviceName.trim() || environmentId;
    yield* startLoop({
      serverUrl,
      deviceName,
      authToken,
      environmentId,
      localHost: localTarget.host,
      localPort: localTarget.port,
    });
  });

  const readCurrentRelaySettings = serverSettings.getSettings.pipe(
    Effect.map((settings) => settings.selfHostedRelay),
    Effect.catch((error) =>
      Effect.logWarning("Could not read self-hosted relay settings.", { error }).pipe(
        Effect.as(null),
      ),
    ),
  );

  const initialRelay = yield* readCurrentRelaySettings;
  let lastSignature = initialRelay ? configSignature(initialRelay) : "";
  if (initialRelay) {
    yield* reconcile(initialRelay, false);
  }

  yield* serverSettings.streamChanges.pipe(
    Stream.map((s) => s.selfHostedRelay),
    Stream.runForEach((next) =>
      Effect.gen(function* () {
        const sig = configSignature(next);
        if (sig === lastSignature) return;
        lastSignature = sig;
        yield* reconcile(next, false);
      }),
    ),
    Effect.ignoreCause({ log: true }),
    Effect.forkIn(serviceScope),
  );

  const getStatus = SubscriptionRef.get(statusRef);
  const subscribeStatus = Stream.concat(
    Stream.fromEffect(SubscriptionRef.get(statusRef)),
    SubscriptionRef.changes(statusRef),
  );

  const connect = Effect.gen(function* () {
    yield* Ref.set(manualConnectRef, true);
    const current = yield* readCurrentRelaySettings;
    if (current) {
      yield* reconcile(current, true);
    }
    return yield* SubscriptionRef.get(statusRef);
  });

  const disconnect = Effect.gen(function* () {
    yield* Ref.set(manualConnectRef, false);
    yield* stopLoop;
    const current = yield* readCurrentRelaySettings;
    const next = current?.enabled ? DISCONNECTED_STATUS : DISABLED_STATUS;
    yield* SubscriptionRef.set(statusRef, next);
    return next;
  });

  return SelfHostedRelayConnector.of({
    getStatus,
    subscribeStatus,
    connect,
    disconnect,
  });
});

export const layer = Layer.effect(SelfHostedRelayConnector, make);
