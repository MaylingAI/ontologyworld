/**
 * Which model decides path -> fileId resolution for a vault document.
 *
 * v1 ("legacy"): `pathToId` is authoritative and is consulted first.
 * v2+ ("id-first"): `meta` is authoritative.  The client resolves purely from
 * it and STOPS WRITING `pathToId` altogether, so whatever entries survive
 * migration are frozen at that instant — never added to, never deleted from,
 * and pointing at the pre-migration fileId space.
 *
 * That distinction is load-bearing in two places, and getting it wrong is
 * silent rather than loud:
 *
 *   - The tombstone reaper treats a `pathToId` reference as proof that a file
 *     is still live.  On a migrated vault that froze stale, so it pinned
 *     bodies that could never be reclaimed — 14 of 46 tombstones on one real
 *     vault.
 *   - getDocumentSummary()'s consistency counters route every check through
 *     `pathToId`.  On a migrated vault they describe a dead map and report a
 *     perfectly healthy 92-file vault as having zero files with text.
 *
 * Both asked the same question, so both now ask it here.
 *
 * v2 and v3 share this model; they differ only in the storage shape of each
 * metadata value (flat JSON versus nested Y.Map), which is irrelevant to path
 * resolution.
 */

import type * as Y from "yjs";

/** Schema version at and above which `meta` is authoritative for paths. */
export const ID_FIRST_PATH_MODEL_MIN_SCHEMA = 2;

/**
 * True when `pathToId` still authorises path resolution.
 *
 * Absent or unreadable schema versions are treated as legacy, which is the
 * conservative answer: it keeps `pathToId` authoritative, so callers veto
 * rather than act on a document whose model they cannot establish.
 */
export function usesLegacyPathModel(doc: Y.Doc): boolean {
	const schemaVersion = doc.getMap("sys").get("schemaVersion");
	return !(
		typeof schemaVersion === "number"
		&& Number.isFinite(schemaVersion)
		&& schemaVersion >= ID_FIRST_PATH_MODEL_MIN_SCHEMA
	);
}
