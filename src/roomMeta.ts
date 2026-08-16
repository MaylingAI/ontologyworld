export const ROOM_META_KEY = "roomMeta";

export interface RoomMeta {
	schemaVersion: number | null;
	updatedAt: string;
}

interface RoomMetaStorageLike {
	get<T = unknown>(key: string): Promise<T | undefined>;
	put<T>(key: string, value: T): Promise<void>;
}

export function isRoomMeta(value: unknown): value is RoomMeta {
	if (typeof value !== "object" || value === null) return false;
	const meta = value as RoomMeta;
	if (meta.schemaVersion !== null && (!Number.isInteger(meta.schemaVersion) || meta.schemaVersion < 0)) {
		return false;
	}
	if (typeof meta.updatedAt !== "string" || meta.updatedAt.length === 0) {
		return false;
	}
	return true;
}

export async function readRoomMeta(
	storage: RoomMetaStorageLike,
): Promise<RoomMeta | null> {
	const raw = await storage.get<unknown>(ROOM_META_KEY);
	if (!isRoomMeta(raw)) return null;
	return raw;
}

export async function writeRoomMeta(
	storage: RoomMetaStorageLike,
	meta: RoomMeta,
): Promise<void> {
	await storage.put(ROOM_META_KEY, meta);
}
