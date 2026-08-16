export const SERVER_VERSION = "0.3.0";

// Compatibility metadata is intentionally explicit so the plugin can reason
// about safe upgrade paths before we add richer release-manifest logic.
export const SERVER_MIN_PLUGIN_VERSION: string | null = null;
export const SERVER_RECOMMENDED_PLUGIN_VERSION = "1.5.0";
export const SERVER_MIN_COMPATIBLE_SERVER_VERSION_FOR_PLUGIN = "0.2.0";
export const SERVER_MIN_COMPATIBLE_PLUGIN_VERSION_FOR_SERVER = "1.3.3";
// The server admits exactly one schema version — the same value the plugin
// writes (`SCHEMA_VERSION` in src/sync/schema.ts). `/api/capabilities`
// publishes it as `schemaVersion` so the plugin can compare its local schema
// against the server's and render an actionable "your server is too old" /
// "your plugin is too old" message instead of a bare WebSocket failure.
// `scripts/guard-schema-version.mjs` enforces that the two stay in lockstep.
export const SERVER_SCHEMA_VERSION = 3;
