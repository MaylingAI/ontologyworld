/**
 * Compaction amplification bench — real SqlDocStore, real PersistenceCoordinator.
 *
 * A/B is the shipped policy against the new one, both running the production
 * classes over a fake SQLite storage:
 *
 *   old — entry pressure relieved by a full checkpoint, fixed byte ceiling.
 *         Reproduced by hiding getSnapshotBytes and reporting the coalesce as
 *         unusable, which is how the coordinator behaved before it could
 *         coalesce at all.
 *   new — entry pressure relieved by coalescing, byte ceiling scaled to the
 *         snapshot.
 *
 * Reports full-document encodes (the O(snapshot) work), bytes rewritten,
 * amplification against content actually typed, wall time, and rows written.
 */
import * as Y from "yjs";
import * as fs from "node:fs";
import * as path from "node:path";
import { SqlDocStore } from "./src/sqlDocStore";
import { PersistenceCoordinator, type DocStore } from "./src/persistenceCoordinator";

const VAULT = "/tmp/yaos-stress-vault";
const SAVES = 1200;

if (!fs.existsSync(VAULT)) {
	console.error(
		`Fixture vault not found at ${VAULT}.\n\n` +
		"These benches need a vault large enough for size effects to show, built from\n" +
		"real markdown rather than synthetic filler.  Create one by duplicating any\n" +
		"existing vault N times:\n\n" +
		"  node -e '\n" +
		"    const fs=require(\"fs\"),path=require(\"path\");\n" +
		"    const SRC=process.env.SRC, DST=\"/tmp/yaos-stress-vault\", COPIES=16;\n" +
		"    const walk=(d,o=[])=>{for(const e of fs.readdirSync(d,{withFileTypes:true})){\n" +
		"      if(e.name.startsWith(\".\"))continue; const p=path.join(d,e.name);\n" +
		"      e.isDirectory()?walk(p,o):e.name.endsWith(\".md\")&&o.push(p);} return o;};\n" +
		"    const files=walk(SRC); fs.rmSync(DST,{recursive:true,force:true});\n" +
		"    for(let c=0;c<COPIES;c++){const dir=path.join(DST,\"copy\"+c);fs.mkdirSync(dir,{recursive:true});\n" +
		"      for(const f of files) fs.writeFileSync(path.join(dir,path.relative(SRC,f).replace(/[/\\\\]/g,\"__\")), fs.readFileSync(f));}\n" +
		"  ' # with SRC=/path/to/vault\n",
	);
	process.exit(1);
}

const CHARS_PER_SAVE = 60;

class FakeSqlCursor<T> {
	constructor(private readonly rows: T[]) {}
	toArray(): T[] { return this.rows; }
	[Symbol.iterator](): Iterator<T> { return this.rows[Symbol.iterator](); }
}

class FakeSqlStorage {
	private tables = new Map<string, Array<Record<string, unknown>>>();
	private autoInc = new Map<string, number>();
	rowsWritten = 0;
	rowsRead = 0;

	exec<T = Record<string, unknown>>(query: string, ...bindings: unknown[]): FakeSqlCursor<T> {
		const q = query.trim().replace(/\s+/g, " ");
		if (q.startsWith("CREATE TABLE IF NOT EXISTS")) {
			const m = q.match(/CREATE TABLE IF NOT EXISTS (\w+)/);
			if (m && !this.tables.has(m[1])) { this.tables.set(m[1], []); this.autoInc.set(m[1], 1); }
			return new FakeSqlCursor<T>([]);
		}
		if (q.startsWith("INSERT INTO snapshot_chunks")) {
			const [i, data] = bindings;
			this.tables.get("snapshot_chunks")!.push({ chunk_index: i, data });
			this.rowsWritten++;
			return new FakeSqlCursor<T>([]);
		}
		if (q.startsWith("INSERT INTO journal")) {
			const [data, len] = bindings;
			const id = this.autoInc.get("journal")!;
			this.autoInc.set("journal", id + 1);
			this.tables.get("journal")!.push({ id, data, byte_length: len });
			this.rowsWritten++;
			return new FakeSqlCursor<T>([]);
		}
		if (q.includes("COUNT(*)") && q.includes("snapshot_chunks")) {
			const t = this.tables.get("snapshot_chunks") ?? [];
			this.rowsRead += t.length;
			const total = t.reduce((s, r) => s + (r.data instanceof ArrayBuffer ? r.data.byteLength : 0), 0);
			return new FakeSqlCursor<T>([{ cnt: t.length, total } as T]);
		}
		if (q.includes("COUNT(*)") && q.includes("journal")) {
			const t = this.tables.get("journal") ?? [];
			this.rowsRead += t.length;
			return new FakeSqlCursor<T>([{ cnt: t.length, total: t.reduce((s, r) => s + (r.byte_length as number), 0) } as T]);
		}
		if (q.startsWith("SELECT data FROM snapshot_chunks")) {
			const t = [...(this.tables.get("snapshot_chunks") ?? [])].sort((a, b) => (a.chunk_index as number) - (b.chunk_index as number));
			this.rowsRead += t.length;
			return new FakeSqlCursor<T>(t as T[]);
		}
		if (q.startsWith("SELECT data, byte_length FROM journal") || q.startsWith("SELECT data FROM journal")) {
			const t = [...(this.tables.get("journal") ?? [])].sort((a, b) => (a.id as number) - (b.id as number));
			this.rowsRead += t.length;
			return new FakeSqlCursor<T>(t as T[]);
		}
		if (q.startsWith("DELETE FROM snapshot_chunks")) {
			this.rowsWritten += (this.tables.get("snapshot_chunks") ?? []).length;
			this.tables.set("snapshot_chunks", []); return new FakeSqlCursor<T>([]);
		}
		if (q.startsWith("DELETE FROM journal")) {
			this.rowsWritten += (this.tables.get("journal") ?? []).length;
			this.tables.set("journal", []); this.autoInc.set("journal", 1); return new FakeSqlCursor<T>([]);
		}
		throw new Error(`unhandled: ${q}`);
	}
}

class FakeStorage {
	sql = new FakeSqlStorage();
	transactionSync<T>(fn: () => T): T { return fn(); }
}
type StoreCtor = ConstructorParameters<typeof SqlDocStore>[0];

function loadFiles(targetBytes: number): Array<{ key: string; text: string }> {
	const out: Array<{ key: string; text: string }> = [];
	let total = 0;
	for (const dir of fs.readdirSync(VAULT).sort()) {
		const d = path.join(VAULT, dir);
		if (!fs.statSync(d).isDirectory()) continue;
		for (const f of fs.readdirSync(d).sort()) {
			const text = fs.readFileSync(path.join(d, f), "utf8");
			out.push({ key: `${dir}/${f}`, text });
			total += Buffer.byteLength(text, "utf8");
			if (total >= targetBytes) return out;
		}
	}
	return out;
}

interface Counters { checkpoints: number; checkpointBytes: number; appends: number; coalesces: number; }

async function run(targetMB: number, policy: "old" | "new"): Promise<{
	encodedBytes: number; contentBytes: number; loopMs: number; c: Counters; rowsWritten: number; replayOk: boolean;
}> {
	const files = loadFiles(targetMB * 1024 * 1024);
	const doc = new Y.Doc();
	const idToText = doc.getMap<Y.Text>("idToText");
	doc.transact(() => {
		for (const f of files) { const t = new Y.Text(); idToText.set(f.key, t); t.insert(0, f.text); }
	});
	const encodedBytes = Y.encodeStateAsUpdate(doc).byteLength;

	const storage = new FakeStorage();
	const real = new SqlDocStore(storage as unknown as StoreCtor);
	const c: Counters = { checkpoints: 0, checkpointBytes: 0, appends: 0, coalesces: 0 };

	// Instrument, and for "old" withhold the two capabilities the new policy
	// depends on — the coordinator then behaves exactly as it did before.
	const store: DocStore = {
		appendUpdate(u) { c.appends++; return real.appendUpdate(u); },
		rewriteCheckpoint(u, sv) { c.checkpoints++; c.checkpointBytes += u.byteLength; return real.rewriteCheckpoint(u, sv); },
		getJournalStats() { return real.getJournalStats(); },
		...(policy === "new"
			? {
				getSnapshotBytes: () => real.getSnapshotBytes(),
				coalesceJournal: () => { c.coalesces++; return real.coalesceJournal(); },
			}
			: {
				// No coalesce available: the coordinator falls through to a full
				// checkpoint, which is the old policy.
				coalesceJournal: () => ({ status: "too-big" as const, stats: real.getJournalStats() }),
			}),
	};

	const coord = new PersistenceCoordinator(doc, store);
	await coord.enqueueSave();                    // seed the checkpoint
	c.checkpoints = 0; c.checkpointBytes = 0; storage.sql.rowsWritten = 0;

	const keys = files.map(f => f.key);
	let seed = 1337;
	const rnd = () => (seed = (seed * 1103515245 + 12345) >>> 0) / 4294967296;
	const chunk = "the quick brown fox jumps over the lazy dog. ".repeat(2).slice(0, CHARS_PER_SAVE);

	let contentBytes = 0;
	const loopStart = performance.now();
	for (let i = 0; i < SAVES; i++) {
		idToText.get(keys[Math.floor(rnd() * keys.length)])!.insert(0, chunk);
		contentBytes += Buffer.byteLength(chunk, "utf8");
		await coord.enqueueSave();
	}
	const loopMs = performance.now() - loopStart;

	// Cold load through the real store must reproduce the live document.
	const fresh = new SqlDocStore(storage as unknown as StoreCtor);
	const state = fresh.loadState();
	const replay = new Y.Doc();
	if (state.snapshot) Y.applyUpdate(replay, state.snapshot);
	for (const u of state.journalUpdates) Y.applyUpdate(replay, u);
	const replayOk = Buffer.from(Y.encodeStateVector(replay)).toString("hex")
		=== Buffer.from(Y.encodeStateVector(doc)).toString("hex");

	coord.dispose(); doc.destroy(); replay.destroy();
	return { encodedBytes, contentBytes, loopMs, c, rowsWritten: storage.sql.rowsWritten, replayOk };
}

console.log(`real SqlDocStore + PersistenceCoordinator, ${SAVES} saves x ${CHARS_PER_SAVE} chars\n`);
console.log("vault".padStart(7) + "  policy" + "  fullEnc".padStart(9) + "  rewrote".padStart(10) +
	"      amp".padStart(10) + "  loopMs".padStart(9) + "  coal".padStart(6) + "  rowsW".padStart(8) + "  replay");
for (const mb of [3, 12, 24, 48]) {
	for (const policy of ["old", "new"] as const) {
		const r = await run(mb, policy);
		const amp = r.contentBytes ? r.c.checkpointBytes / r.contentBytes : 0;
		console.log(
			((r.encodedBytes / 1048576).toFixed(1) + "MB").padStart(7) + "  " + policy.padEnd(6) +
			String(r.c.checkpoints).padStart(9) +
			((r.c.checkpointBytes / 1048576).toFixed(0) + "MB").padStart(10) +
			(amp ? amp.toFixed(0) + "x" : "-").padStart(10) +
			r.loopMs.toFixed(0).padStart(9) +
			String(r.c.coalesces).padStart(6) +
			String(r.rowsWritten).padStart(8) +
			"  " + (r.replayOk ? "MATCH" : "MISMATCH"),
		);
	}
}
