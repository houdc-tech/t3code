import { useState } from "react";
import { T, useGT, useMessages } from "gt-react";
import {
  AuthRelayConfigureScope,
  AuthRelayReadScope,
  AuthRelayWriteScope,
  DEFAULT_SELF_HOSTED_RELAY_SETTINGS,
  type EnvironmentId,
  type SelfHostedRelaySettings as SelfHostedRelaySettingsValue,
  type SelfHostedRelayStatus,
} from "@t3tools/contracts";
import {
  isAtomCommandInterrupted,
  squashAtomCommandFailure,
} from "@t3tools/client-runtime/state/runtime";
import { thrownErrorMessage } from "@t3tools/client-runtime/errors";

import { SettingsRow } from "./settingsLayout";
import { Button } from "../ui/button";
import { Input } from "../ui/input";
import { Switch } from "../ui/switch";
import { stackedThreadToast, toastManager } from "../ui/toast";
import { useEnvironmentScope } from "~/state/session";
import { useEnvironmentQuery } from "~/state/query";
import { serverEnvironment } from "~/state/server";
import { useAtomCommand } from "~/state/use-atom-command";
import { useUpdateEnvironmentSettings } from "~/hooks/useSettings";

const SECRET_REDACTED_MARKER = "\u2022\u2022\u2022\u2022\u2022\u2022";

function statusSummary(
  status: SelfHostedRelayStatus | null,
  gt: ReturnType<typeof useGT>,
): { readonly label: string; readonly tone: "muted" | "success" | "error" } {
  if (!status) {
    return { label: gt("Loading…"), tone: "muted" };
  }
  switch (status.phase) {
    case "connected":
      return {
        label: status.publicHttpBaseUrl
          ? gt("Connected · {url}", { url: status.publicHttpBaseUrl })
          : gt("Connected"),
        tone: "success",
      };
    case "connecting":
      return { label: gt("Connecting…"), tone: "muted" };
    case "disconnected":
      return { label: gt("Disconnected"), tone: "muted" };
    case "disabled":
      return { label: gt("Off"), tone: "muted" };
    case "error":
      return {
        label: status.lastError
          ? gt("Error: {error}", { error: status.lastError })
          : gt("Connection error"),
        tone: "error",
      };
  }
}

export function SelfHostedRelaySettings({
  environmentId,
  settings,
  status,
}: {
  readonly environmentId: EnvironmentId;
  readonly settings: SelfHostedRelaySettingsValue | undefined;
  readonly status: SelfHostedRelayStatus | null;
}) {
  const gt = useGT();
  const messages = useMessages();
  const canRead = useEnvironmentScope(environmentId, AuthRelayReadScope);
  const canConfigure = useEnvironmentScope(environmentId, AuthRelayConfigureScope);
  const canControl = useEnvironmentScope(environmentId, AuthRelayWriteScope);
  const canEditRelaySettings = canConfigure && canControl;
  const updateSettings = useUpdateEnvironmentSettings(environmentId);
  const connectRelay = useAtomCommand(serverEnvironment.connectSelfHostedRelay, {
    reportFailure: false,
  });
  const disconnectRelay = useAtomCommand(serverEnvironment.disconnectSelfHostedRelay, {
    reportFailure: false,
  });

  const saved = settings ?? DEFAULT_SELF_HOSTED_RELAY_SETTINGS;
  const savedHasCredential =
    typeof saved.authCredential === "string"
      ? saved.authCredential.length > 0
      : saved.authCredential !== undefined;

  const [expanded, setExpanded] = useState(saved.enabled);
  const [serverUrl, setServerUrl] = useState(saved.serverUrl);
  const [deviceName, setDeviceName] = useState(saved.deviceName);
  const [authCredential, setAuthCredential] = useState("");
  const [autoConnect, setAutoConnect] = useState(saved.autoConnect);
  const [saving, setSaving] = useState(false);
  const [controlling, setControlling] = useState(false);

  if (!canRead && !canConfigure && !canControl) {
    return null;
  }

  const summary = statusSummary(status, gt);
  const serverUrlChanged = serverUrl.trim() !== saved.serverUrl.trim();
  const isDirty =
    serverUrlChanged ||
    deviceName.trim() !== saved.deviceName.trim() ||
    autoConnect !== saved.autoConnect ||
    authCredential.trim().length > 0;

  const handleToggleEnabled = (enabled: boolean) => {
    if (!canEditRelaySettings || saving) return;
    if (enabled) setExpanded(true);
    setSaving(true);
    updateSettings(
      { selfHostedRelay: { enabled } },
      {
        onSaved: () => setSaving(false),
        onFailed: () => setSaving(false),
      },
    );
  };

  const handleSave = () => {
    if (!canEditRelaySettings || saving) return;
    setSaving(true);
    const nextCredential =
      authCredential.trim().length > 0
        ? authCredential.trim()
        : savedHasCredential && !serverUrlChanged
          ? SECRET_REDACTED_MARKER
          : "";
    updateSettings(
      {
        selfHostedRelay: {
          enabled: saved.enabled,
          serverUrl: serverUrl.trim(),
          deviceName: deviceName.trim(),
          authCredential: nextCredential,
          autoConnect,
        },
      },
      {
        onSaved: () => {
          setSaving(false);
          setAuthCredential("");
          toastManager.add({
            type: "success",
            title: gt("Self-hosted Relay saved"),
          });
        },
        onFailed: () => {
          setSaving(false);
        },
      },
    );
  };

  const runRelayControl = async (action: "connect" | "disconnect") => {
    if (!canControl || controlling) return;
    setControlling(true);
    const result =
      action === "disconnect"
        ? await disconnectRelay({ environmentId, input: {} })
        : await connectRelay({ environmentId, input: {} });
    setControlling(false);
    if (result._tag === "Failure" && !isAtomCommandInterrupted(result)) {
      const error = squashAtomCommandFailure(result);
      toastManager.add(
        stackedThreadToast({
          type: "error",
          title:
            action === "disconnect"
              ? gt("Could not disconnect relay")
              : gt("Could not connect relay"),
          description:
            error instanceof Error
              ? (thrownErrorMessage(error, messages) ?? error.message)
              : gt("Action failed."),
        }),
      );
    }
  };

  return (
    <>
      <SettingsRow
        title={gt("Self-hosted Relay")}
        description={
          canRead ? (
            <span
              className={
                summary.tone === "error"
                  ? "text-destructive"
                  : summary.tone === "success"
                    ? "text-success-foreground"
                    : undefined
              }
            >
              {summary.label}
            </span>
          ) : (
            gt("Relay status unavailable for this session.")
          )
        }
        control={
          <div className="flex items-center gap-2">
            <Button size="sm" variant="ghost" onClick={() => setExpanded((value) => !value)}>
              {expanded ? gt("Hide") : gt("Configure")}
            </Button>
            {canEditRelaySettings ? (
              <Switch
                checked={saved.enabled}
                disabled={saving}
                onCheckedChange={handleToggleEnabled}
                aria-label={gt("Enable Self-hosted Relay")}
              />
            ) : null}
          </div>
        }
      />
      {expanded ? (
        <div className="space-y-3 border-t border-border/50 bg-muted/20 px-4 py-3 text-sm">
          <div className="grid gap-3 sm:grid-cols-2">
            <label className="space-y-1">
              <span className="text-xs font-medium text-muted-foreground">
                <T>Relay server URL</T>
              </span>
              <Input
                value={serverUrl}
                disabled={!canEditRelaySettings || saving}
                placeholder="https://mac.relay.example.com"
                onChange={(event) => setServerUrl(event.target.value)}
              />
            </label>
            <label className="space-y-1">
              <span className="text-xs font-medium text-muted-foreground">
                <T>Device name</T>
              </span>
              <Input
                value={deviceName}
                disabled={!canEditRelaySettings || saving}
                placeholder="mac-studio"
                onChange={(event) => setDeviceName(event.target.value)}
              />
            </label>
          </div>
          <label className="block space-y-1">
            <span className="text-xs font-medium text-muted-foreground">
              <T>Authentication credential</T>
            </span>
            <Input
              type="password"
              value={authCredential}
              disabled={!canEditRelaySettings || saving}
              placeholder={
                savedHasCredential && !serverUrlChanged
                  ? gt("Saved (leave blank to keep current)")
                  : gt("Enter connector bearer token")
              }
              onChange={(event) => setAuthCredential(event.target.value)}
            />
          </label>
          <div className="flex flex-wrap items-center justify-between gap-3 pt-1">
            <label className="flex items-center gap-2 text-xs text-muted-foreground">
              <Switch
                checked={autoConnect}
                disabled={!canEditRelaySettings || saving}
                onCheckedChange={setAutoConnect}
                aria-label={gt("Auto-connect on startup")}
              />
              <span>
                <T>Auto-connect when server starts</T>
              </span>
            </label>
            <div className="flex items-center gap-2">
              {canControl && saved.enabled ? (
                canRead ? (
                  <Button
                    size="sm"
                    variant="outline"
                    disabled={controlling || saving}
                    onClick={() =>
                      void runRelayControl(
                        status?.phase === "connected" || status?.phase === "connecting"
                          ? "disconnect"
                          : "connect",
                      )
                    }
                  >
                    {status?.phase === "connected" || status?.phase === "connecting"
                      ? gt("Disconnect")
                      : gt("Connect")}
                  </Button>
                ) : (
                  <>
                    <Button
                      size="sm"
                      variant="outline"
                      disabled={controlling || saving}
                      onClick={() => void runRelayControl("connect")}
                    >
                      {gt("Connect")}
                    </Button>
                    <Button
                      size="sm"
                      variant="outline"
                      disabled={controlling || saving}
                      onClick={() => void runRelayControl("disconnect")}
                    >
                      {gt("Disconnect")}
                    </Button>
                  </>
                )
              ) : null}
              {canEditRelaySettings ? (
                <Button size="sm" disabled={!isDirty || saving} onClick={handleSave}>
                  {saving ? gt("Saving…") : gt("Save")}
                </Button>
              ) : null}
            </div>
          </div>
        </div>
      ) : null}
    </>
  );
}

export function useSelfHostedRelayStatus(
  environmentId: EnvironmentId | null,
  supported: boolean,
  canRead: boolean,
): SelfHostedRelayStatus | null {
  const messages = useMessages();
  const query = useEnvironmentQuery(
    environmentId !== null && supported && canRead
      ? serverEnvironment.selfHostedRelayStatus({ environmentId, input: {} })
      : null,
    messages,
  );
  return query.data ?? null;
}
