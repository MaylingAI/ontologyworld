/**
 * SqlDocStore: Native SQLite storage for Y.Doc persistence.
 *
 * The only document store.  It keeps state in direct SQLite tables, which the
 * DO SQLite API backs with:
 * - ACID transactions via ctx.storage.transactionSync()
 * - No per-key batching gymnastics
 * - WAL-based write-ahead logging (CF handles this internally)
 *
 * Schema:
 *   snapshot_chunks — the compacted Y.Doc state, split into ≤1MB rows
 *   journal         — append-only delta log between compactions
 *
 * IMPORTANT — Memory contract for BLOB bindings:
 * Cloudflare's sql.exec() accepts ArrayBuffer for BLOB columns.  When
 * passing binary data, we MUST use .slice() (not .subarray()) to produce
 * an independent ArrayBuffer.  Using .subarray() shares the parent buffer
 * and .buffer would point to the ENTIRE underlying allocation, causing
 * silent data corruption or SQLITE_TOOBIG errors.
 *
 * The helper `ownedBuffer()` below enforces this contract.
 */

import * as Y from "yjs";

/** Max bytes per snapshot chunk row.  Well under the 2MB SQLite value limit. */
const SNAPSHOT_CHUNK_SIZE = 1 * 1024 * 1024; // 1MB

/**
 * Max bytes for a single journal row.  Must be under the 2MB SQLite BLOB limit.
 * We use 1.5MB to leave margin for encoding overhead.
 */
const MAX_JOURNAL_ENTRY_BYTES = 1.5 * 1024 * 1024;

/**
 * Create an owned ArrayBuffer from a Uint8Array slice.
 *
 * This is the ONLY correct way to produce an ArrayBuffer for sql.exec() BLOB
 * bindings.  It guarantees the buffer contains exactly the intended bytes and
 * nothing else.
 *
 * DO NOT replace this with .subarray().buffer — that returns the full parent
 * buffer and will silently write wrong data to the database.
 */
function ownedBuffer(bytes: Uint8Array, start: number, end: number): ArrayBuffer {
	return bytes.slice(start, end).buffer;
}

export interface JournalStats {
	entryCount: number;
	totalBytes: number;
}

export interface LoadedDocState {
	snapshot: Uint8Array | null;
	journalUpdates: Uint8Array[];
	journalStats: JournalStats;
}

/**
 * Outcome of a journal coalesce.
 *
 * `too-big` and `failed` are not errors the caller should swallow: both mean
 * the entry count is still high, so the caller must escalate to a checkpoint
 * rather than assume the pressure was relieved.
 */
export interface CoalesceResult {
	status: "ok" | "noop" | "too-big" | "failed";
	stats: JournalStats;
}

/**
 * The slice of DO SQLite this store depends on: one query method and a cursor
 * that can be listed or iterated. Declaring the dependency narrowly (rather
 * than taking the platform's `SqlStorage`) is what lets a test drive the store
 * with an in-memory table set.
 *
 * The row-type constraint mirrors the platform signature exactly. It has to:
 * with an unconstrained `T`, a real `DurableObjectStorage` was NOT assignable
 * to this port — its `exec` cannot produce a cursor of an arbitrary row type —
 * so the one production construction site had to launder it through `as any`.
 * Every column this store reads is an ArrayBuffer or a number, so the
 * constraint costs the callers nothing.
 */
interface SqlStorage {
	exec<T extends Record<string, SqlStorageValue>>(query: string, ...bindings: unknown[]): SqlStorageCursor<T>;
}

interface SqlStorageCursor<T> {
	toArray(): T[];
	[Symbol.iterator](): Iterator<T>;
}

interface DurableObjectStorageWithSql {
	sql: SqlStorage;
	transactionSync<T>(closure: () => T): T;
}

/**
 * SqlDocStore provides Y.Doc persistence using native DO SQLite.
 */
export class SqlDocStore {
	private initialized = false;

	/**
	 * In-memory mirror of the journal aggregate.
	 *
	 * These are EXACT, not approximate: the Durable Object is single threaded
	 * and this class is the sole writer of the `journal` table, so nothing can
	 * change it behind our back between our own writes.  Tracking the aggregate
	 * here turns getJournalStats() — called on every save — from a full table
	 * scan (SQLite has no O(1) COUNT, and SUM must touch every row) into O(1).
	 * On a SQLite-backed DO, rows touched are the billed unit, so the scan cost
	 * roughly the journal length in rows read per save.
	 */
	private journalEntryCount = 0;
	private journalTotalBytes = 0;
	private journalStatsSeeded = false;

	/**
	 * Size of the persisted snapshot, mirrored for the same reason as the
	 * journal aggregate.  A compaction trigger that does not know this number
	 * cannot bound write amplification: rewriting the checkpoint costs O(this),
	 * so a threshold expressed only in journal terms rewrites the whole vault
	 * for a few KB of deltas, and does it proportionally more often the larger
	 * the vault gets.  Zero until a load or a checkpoint establishes it, which
	 * callers must read as "unknown" rather than "empty".
	 */
	private snapshotByteCount = 0;

	constructor(private readonly storage: DurableObjectStorageWithSql) {}

	/**
	 * Seed the journal counters from storage, at most once per instance.
	 *
	 * loadState() seeds them for free from the scan it already performs, so
	 * this aggregate only runs when stats are requested before any load —
	 * and then the answer is cached for the life of the instance.
	 */
	private ensureJournalStatsSeeded(): void {
		if (this.journalStatsSeeded) return;
		const rows = this.storage.sql.exec<{ cnt: number; total: number }>(
			"SELECT COUNT(*) as cnt, COALESCE(SUM(byte_length), 0) as total FROM journal",
		).toArray();
		const row = rows[0];
		this.journalEntryCount = row?.cnt ?? 0;
		this.journalTotalBytes = row?.total ?? 0;
		this.journalStatsSeeded = true;
	}

	private ensureSchema(): void {
		if (this.initialized) return;
		this.storage.sql.exec(`
			CREATE TABLE IF NOT EXISTS snapshot_chunks (
				chunk_index INTEGER PRIMARY KEY,
				data BLOB NOT NULL
			)
		`);
		this.storage.sql.exec(`
			CREATE TABLE IF NOT EXISTS journal (
				id INTEGER PRIMARY KEY AUTOINCREMENT,
				data BLOB NOT NULL,
				byte_length INTEGER NOT NULL,
				created_at TEXT NOT NULL DEFAULT (datetime('now'))
			)
		`);
		this.initialized = true;
	}

	/**
	 * Load the full document state: snapshot + journal replay.
	 *
	 * MEMORY CONTRACT — snapshot reassembly:
	 * The snapshot must end up as one contiguous Uint8Array because a Yjs
	 * update cannot be applied in fragments.  That contiguous buffer costs S
	 * bytes.  The naive implementation additionally materialises every row via
	 * .toArray() before copying, holding another S bytes of per-row
	 * ArrayBuffers alive for the whole copy loop — peak 2S on a 128MB heap
	 * that the decoded Y.Doc will then dominate.
	 *
	 * Instead we size the buffer with an aggregate query, then stream the
	 * cursor and copy each row straight into place.  Once a row is copied its
	 * ArrayBuffer is unreachable and collectable, so peak is S + one chunk
	 * rather than 2S.
	 *
	 * The cursor is consumed synchronously with no intervening await, as
	 * required for snapshot isolation (a cursor resumed after an await may
	 * observe later, possibly uncommitted, writes).
	 */
	loadState(): LoadedDocState {
		this.ensureSchema();

		const [sizes] = this.storage.sql.exec<{ cnt: number; total: number }>(
			"SELECT COUNT(*) AS cnt, COALESCE(SUM(LENGTH(data)), 0) AS total FROM snapshot_chunks",
		).toArray();

		this.snapshotByteCount = sizes?.total ?? 0;
		let snapshot: Uint8Array | null = null;
		if (sizes && sizes.cnt > 0) {
			snapshot = new Uint8Array(sizes.total);
			let offset = 0;
			const cursor = this.storage.sql.exec<{ data: ArrayBuffer }>(
				"SELECT data FROM snapshot_chunks ORDER BY chunk_index",
			);
			for (const row of cursor) {
				const chunk = new Uint8Array(row.data);
				if (offset + chunk.byteLength > snapshot.byteLength) {
					// A concurrent rewriteCheckpoint cannot interleave here (single
					// threaded, no await), so this means the aggregate and the scan
					// disagree — fail loudly rather than persist a truncated doc.
					throw new Error(
						`snapshot_chunks size mismatch: aggregate reported ${snapshot.byteLength} bytes, scan produced at least ${offset + chunk.byteLength}`,
					);
				}
				snapshot.set(chunk, offset);
				offset += chunk.byteLength;
			}
			if (offset !== snapshot.byteLength) {
				throw new Error(
					`snapshot_chunks size mismatch: aggregate reported ${snapshot.byteLength} bytes, scan produced ${offset}`,
				);
			}
		}

		// Read journal
		const journalRows = this.storage.sql.exec<{ data: ArrayBuffer; byte_length: number }>(
			"SELECT data, byte_length FROM journal ORDER BY id",
		).toArray();

		const journalUpdates: Uint8Array[] = [];
		let totalBytes = 0;
		for (const row of journalRows) {
			journalUpdates.push(new Uint8Array(row.data));
			totalBytes += row.byte_length;
		}

		// This scan already visited every journal row, so the aggregate is free
		// here — seed the counters instead of paying for a separate query later.
		this.journalEntryCount = journalUpdates.length;
		this.journalTotalBytes = totalBytes;
		this.journalStatsSeeded = true;

		return {
			snapshot,
			// Deliberately NOT merged here.  Y.mergeUpdates decodes and re-encodes
			// every entry, which costs far more than the incremental applyUpdate
			// calls it saves: measured at 12MB with 4,000 journal rows, merging on
			// read took 143ms to save 10ms of apply — an 8x regression in total
			// cold-load time.  Merging is worth doing once at WRITE time, where it
			// is amortised over the saves that produced the entries and leaves a
			// one-row journal for every subsequent load.  See coalesceJournal.
			journalUpdates,
			journalStats: {
				entryCount: journalUpdates.length,
				totalBytes,
			},
		};
	}

	/**
	 * Append a Y.Doc update to the journal.
	 *
	 * If the update exceeds MAX_JOURNAL_ENTRY_BYTES, returns `null` to signal
	 * that the caller should use rewriteCheckpoint instead.  This avoids
	 * hitting SQLITE_TOOBIG on large deltas (e.g., pasting a multi-MB document).
	 */
	appendUpdate(update: Uint8Array): JournalStats | null {
		this.ensureSchema();

		if (update.byteLength === 0) {
			return this.getJournalStats();
		}

		// Explicit size guard: do not attempt INSERT for payloads that would
		// exceed the SQLite per-value BLOB limit.  The caller (PersistenceCoordinator)
		// must fall back to rewriteCheckpoint for oversized deltas.
		if (update.byteLength > MAX_JOURNAL_ENTRY_BYTES) {
			return null;
		}

		// Seed before the INSERT: a lazy seed afterwards would already include
		// this row and the increment below would double-count it.
		this.ensureJournalStatsSeeded();

		this.storage.sql.exec(
			"INSERT INTO journal (data, byte_length) VALUES (?, ?)",
			ownedBuffer(update, 0, update.byteLength),
			update.byteLength,
		);

		this.journalEntryCount++;
		this.journalTotalBytes += update.byteLength;

		return this.getJournalStats();
	}

	/**
	 * Rewrite the checkpoint: atomically replace the snapshot and clear the journal.
	 * The stateVector parameter is accepted for interface compatibility but not stored
	 * separately (it's embedded in the Y.Doc update).
	 */
	rewriteCheckpoint(update: Uint8Array, _stateVector?: Uint8Array): void {
		this.ensureSchema();

		const bytes = update;
		const chunkCount = bytes.byteLength === 0
			? 0
			: Math.ceil(bytes.byteLength / SNAPSHOT_CHUNK_SIZE);

		this.storage.transactionSync(() => {
			// Clear old snapshot and journal
			this.storage.sql.exec("DELETE FROM snapshot_chunks");
			this.storage.sql.exec("DELETE FROM journal");

			// Write new snapshot chunks using ownedBuffer for safe BLOB binding
			for (let i = 0; i < chunkCount; i++) {
				const start = i * SNAPSHOT_CHUNK_SIZE;
				const end = Math.min(start + SNAPSHOT_CHUNK_SIZE, bytes.byteLength);
				this.storage.sql.exec(
					"INSERT INTO snapshot_chunks (chunk_index, data) VALUES (?, ?)",
					i,
					ownedBuffer(bytes, start, end),
				);
			}
		});

		// The journal is now empty.  Set this only after the transaction
		// commits: a rollback throws out of transactionSync, leaving the
		// counters describing the journal that is still on disk.
		this.journalEntryCount = 0;
		this.journalTotalBytes = 0;
		this.journalStatsSeeded = true;
		this.snapshotByteCount = update.byteLength;
	}

	/**
	 * Get current journal statistics.
	 */
	getJournalStats(): JournalStats {
		this.ensureSchema();
		this.ensureJournalStatsSeeded();

		return {
			entryCount: this.journalEntryCount,
			totalBytes: this.journalTotalBytes,
		};
	}

	/** Bytes of the persisted snapshot; 0 means "not yet known". */
	getSnapshotBytes(): number {
		this.ensureSchema();
		return this.snapshotByteCount;
	}

	/**
	 * Replace every journal row with a single equivalent row.
	 *
	 * This is the cheap half of compaction.  Rewriting the checkpoint costs
	 * O(snapshot) because it re-encodes the whole document; coalescing costs
	 * O(journal), which is smaller by orders of magnitude on any vault big
	 * enough to care.  Both relieve the same pressure — a journal with too many
	 * entries — so paying the expensive one for it is a mistake the entry-count
	 * trigger has been making on every 51st save regardless of vault size.
	 *
	 * Cold load gets the same benefit: replay is dominated by the number of
	 * applyUpdate calls, so a one-entry journal loads ~4x faster than the same
	 * bytes spread over thousands of rows, and with lower peak memory.
	 *
	 * Correctness rests on Yjs guaranteeing that merged updates apply
	 * identically to the originals, so the persisted state is unchanged and the
	 * coordinator's `lastPersistedStateVector` stays valid.  Callers MUST NOT
	 * treat this as a save: it persists nothing new.
	 */
	coalesceJournal(): CoalesceResult {
		this.ensureSchema();
		this.ensureJournalStatsSeeded();

		if (this.journalEntryCount <= 1) {
			return { status: "noop", stats: this.getJournalStats() };
		}

		const rows = this.storage.sql.exec<{ data: ArrayBuffer }>(
			"SELECT data FROM journal ORDER BY id",
		).toArray();

		let merged: Uint8Array;
		try {
			merged = Y.mergeUpdates(rows.map((r) => new Uint8Array(r.data)));
		} catch {
			// A malformed entry must not cost the caller its journal.  Leaving the
			// rows alone keeps the slower replay path working; the byte-triggered
			// checkpoint will clear them eventually.
			return { status: "failed", stats: this.getJournalStats() };
		}

		// Merging usually shrinks the payload, but it is not guaranteed to, and a
		// row over the BLOB limit would fail the INSERT inside the transaction.
		// Report it so the caller escalates to a checkpoint instead.
		if (merged.byteLength > MAX_JOURNAL_ENTRY_BYTES) {
			return { status: "too-big", stats: this.getJournalStats() };
		}

		this.storage.transactionSync(() => {
			this.storage.sql.exec("DELETE FROM journal");
			this.storage.sql.exec(
				"INSERT INTO journal (data, byte_length) VALUES (?, ?)",
				ownedBuffer(merged, 0, merged.byteLength),
				merged.byteLength,
			);
		});

		// Only after the commit, for the same reason as rewriteCheckpoint: a
		// rollback throws out of transactionSync and the old rows survive.
		this.journalEntryCount = 1;
		this.journalTotalBytes = merged.byteLength;

		return { status: "ok", stats: this.getJournalStats() };
	}
}
