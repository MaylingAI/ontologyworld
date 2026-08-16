import * as Y from "yjs";
import { MAX_SV_ECHO_BASE64_BYTES, SV_ECHO_SCHEMA, SV_ECHO_TYPE } from "./svEchoProtocol";

// y-partyserver custom-message wire prefix. Verified in
// docs/archive/server-ack-spike.md against y-partyserver@2.1.2; rerun that
// spike and tests/live/provider-manual-connect.mjs when upgrading y-partyserver.
const Y_PARTYSERVER_CUSTOM_PREFIX = "__YPS:";
const WS_READY_STATE_OPEN = 1;

export type SvEchoKind = "baseline" | "postApply";

/**
 * Durability marker carried alongside the state vector.
 *
 * The state vector alone cannot express "your change is stored": it tracks
 * insert clocks, so a deletion-only change leaves it byte-identical and any
 * echo appears to confirm it.  `generation` advances only on a successful
 * persist, and `epoch` identifies the coordinator instance it belongs to so a
 * client can tell a restart (counter back to zero) from progress.
 *
 * Both are OPTIONAL and the schema is deliberately NOT bumped.  Clients reject
 * on strict schema equality (src/sync/svEchoMessage.ts), so a bump would make
 * every existing client discard the echo entirely and lose receipts it
 * currently has.  Old clients ignore unknown fields instead.
 */
export interface SvEchoDurability {
	generation: number;
	epoch: string;
	/**
	 * True when the room's persistence is degraded.
	 *
	 * Carried here because the echo is the only channel the client already
	 * receives on every update-bearing message and on connect.  Without it a
	 * client has no way to learn that the server cannot store its writes short
	 * of polling the debug endpoint, so the failure is invisible to the user —
	 * which is the entire class of bug this addresses.
	 *
	 * Emitted only when true, keeping the common payload unchanged.
	 */
	degraded?: boolean;
}

export type SvEchoSendResult =
	| { ok: true; kind: SvEchoKind; bytes: number }
	| { ok: false; kind: SvEchoKind; bytes: number; failure: "not_open" | "oversize" | "send_failed" };

type SendableConnection = {
	readyState?: number;
	send(message: string): void;
};

function encodeBytesBase64(bytes: Uint8Array): string {
	let s = "";
	for (let i = 0; i < bytes.length; i += 8192) {
		s += String.fromCharCode(...bytes.subarray(i, i + 8192));
	}
	return btoa(s);
}

function framedByteLength(payload: string): number {
	return new TextEncoder().encode(`${Y_PARTYSERVER_CUSTOM_PREFIX}${payload}`).byteLength;
}

function svEchoPayload(encodedSv: string, durability?: SvEchoDurability): string {
	return JSON.stringify(
		durability
			? {
				type: SV_ECHO_TYPE,
				schema: SV_ECHO_SCHEMA,
				sv: encodedSv,
				gen: durability.generation,
				genEpoch: durability.epoch,
				...(durability.degraded ? { degraded: true } : {}),
			}
			: {
				type: SV_ECHO_TYPE,
				schema: SV_ECHO_SCHEMA,
				sv: encodedSv,
			},
	);
}

export function makeSvEchoCustomMessage(
	serverSv: Uint8Array,
	durability?: SvEchoDurability,
): string {
	return svEchoPayload(encodeBytesBase64(serverSv), durability);
}

export function makeSvEchoCustomMessageForDoc(doc: Y.Doc, durability?: SvEchoDurability): string {
	return makeSvEchoCustomMessage(Y.encodeStateVector(doc), durability);
}

export function trySendSvEchoStateVector(
	connection: SendableConnection,
	serverSv: Uint8Array,
	kind: SvEchoKind,
	durability?: SvEchoDurability,
): SvEchoSendResult {
	if (connection.readyState !== undefined && connection.readyState !== WS_READY_STATE_OPEN) {
		return { ok: false, kind, bytes: 0, failure: "not_open" };
	}

	const encodedSv = encodeBytesBase64(serverSv);
	if (encodedSv.length > MAX_SV_ECHO_BASE64_BYTES) {
		const bytes = framedByteLength(svEchoPayload(encodedSv, durability));
		return { ok: false, kind, bytes, failure: "oversize" };
	}

	const payload = svEchoPayload(encodedSv, durability);
	const framedMessage = `${Y_PARTYSERVER_CUSTOM_PREFIX}${payload}`;
	const bytes = new TextEncoder().encode(framedMessage).byteLength;

	try {
		connection.send(framedMessage);
		return { ok: true, kind, bytes };
	} catch {
		return { ok: false, kind, bytes, failure: "send_failed" };
	}
}

export function trySendSvEcho(
	connection: SendableConnection,
	doc: Y.Doc,
	kind: SvEchoKind,
	durability?: SvEchoDurability,
): SvEchoSendResult {
	return trySendSvEchoStateVector(connection, Y.encodeStateVector(doc), kind, durability);
}
