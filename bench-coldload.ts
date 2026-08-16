/**
 * Cold-load scale bench.
 *
 * Measures what a Durable Object pays to wake up: reassemble the snapshot,
 * decode it, then replay the journal.  Sweeps snapshot size S and journal
 * entry count N so the per-entry overhead can be separated from the per-byte
 * cost — which is the number that decides how large a journal is safe to keep,
 * i.e. K in `journalBytes > S/K`.
 *
 * Also compares replaying N separate journal entries against one merged entry
 * holding identical content, since that is the cold-load side of coalescing.
 *
 * Uses the real SqlDocStore.loadState() against a fake SQLite storage.
 */
import * as Y from "yjs";
import * as fs from "node:fs";
import * as path from "node:path";
import { SqlDocStore } from "./src/sqlDocStore";

const VAULT = "/tmp/yaos-stress-vault";

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


class FakeSqlCursor<T> {
	constructor(private readonly rows: T[]) {}
	toArray(): T[] { return this.rows; }
	[Symbol.iterator](): Iterator<T> { return this.rows[Symbol.iterator](); }
}

class FakeSqlStorage {
	private tables = new Map<string, Array<Record<string, unknown>>>();
	private autoInc = new Map<string, number>();
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
			return new FakeSqlCursor<T>([]);
		}
		if (q.startsWith("INSERT INTO journal")) {
			const [data, len] = bindings;
			const id = this.autoInc.get("journal")!;
			this.autoInc.set("journal", id + 1);
			this.tables.get("journal")!.push({ id, data, byte_length: len });
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
		if (q.startsWith("SELECT data, byte_length FROM journal")) {
			const t = [...(this.tables.get("journal") ?? [])].sort((a, b) => (a.id as number) - (b.id as number));
			this.rowsRead += t.length;
			return new FakeSqlCursor<T>(t as T[]);
		}
		if (q.startsWith("DELETE FROM snapshot_chunks")) { this.tables.set("snapshot_chunks", []); return new FakeSqlCursor<T>([]); }
		if (q.startsWith("DELETE FROM journal")) { this.tables.set("journal", []); this.autoInc.set("journal", 1); return new FakeSqlCursor<T>([]); }
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

function heap(): number { const m = process.memoryUsage(); return m.heapUsed + m.arrayBuffers; }

interface ColdLoad {
	loadStateMs: number;
	snapshotApplyMs: number;
	journalApplyMs: number;
	totalMs: number;
	peakMiB: number;
	snapshotBytes: number;
	journalBytes: number;
	journalEntries: number;
}

/** Build a persisted state, then cold-load it exactly as the DO would. */
function coldLoad(targetMB: number, edits: number, coalesced: boolean): ColdLoad {
	const files = loadFiles(targetMB * 1024 * 1024);
	const doc = new Y.Doc();
	const idToText = doc.getMap<Y.Text>("idToText");
	doc.transact(() => {
		for (const f of files) { const t = new Y.Text(); idToText.set(f.key, t); t.insert(0, f.text); }
	});

	const storage = new FakeStorage();
	const store = new SqlDocStore(storage as unknown as StoreCtor);
	store.rewriteCheckpoint(Y.encodeStateAsUpdate(doc));

	// Produce `edits` journal deltas of realistic size.
	const keys = files.map(f => f.key);
	let seed = 7; const rnd = () => (seed = (seed * 1103515245 + 12345) >>> 0) / 4294967296;
	const chunk = "the quick brown fox jumps over the lazy dog. ".repeat(2).slice(0, 60);
	let sv = Y.encodeStateVector(doc);
	const deltas: Uint8Array[] = [];
	for (let i = 0; i < edits; i++) {
		idToText.get(keys[Math.floor(rnd() * keys.length)])!.insert(0, chunk);
		const d = Y.encodeStateAsUpdate(doc, sv);
		deltas.push(d);
		sv = Y.encodeStateVector(doc);
	}
	if (coalesced) {
		if (deltas.length) store.appendUpdate(Y.mergeUpdates(deltas));
	} else {
		for (const d of deltas) store.appendUpdate(d);
	}
	const liveSv = Buffer.from(Y.encodeStateVector(doc)).toString("hex");
	doc.destroy();

	// ---- cold load, fresh instance over the same storage ----
	if (global.gc) { global.gc(); global.gc(); }
	const base = heap();
	let peak = base;
	const track = () => { const h = heap(); if (h > peak) peak = h; };

	const fresh = new SqlDocStore(storage as unknown as StoreCtor);
	let t = performance.now();
	const state = fresh.loadState();
	const loadStateMs = performance.now() - t;
	track();

	const rebuilt = new Y.Doc();
	t = performance.now();
	if (state.snapshot) Y.applyUpdate(rebuilt, state.snapshot);
	const snapshotApplyMs = performance.now() - t;
	track();

	t = performance.now();
	for (const u of state.journalUpdates) Y.applyUpdate(rebuilt, u);
	const journalApplyMs = performance.now() - t;
	track();

	const rebuiltSv = Buffer.from(Y.encodeStateVector(rebuilt)).toString("hex");
	if (rebuiltSv !== liveSv) throw new Error("cold load did not reproduce live state");
	rebuilt.destroy();

	return {
		loadStateMs, snapshotApplyMs, journalApplyMs,
		totalMs: loadStateMs + snapshotApplyMs + journalApplyMs,
		peakMiB: (peak - base) / 1048576,
		snapshotBytes: state.snapshot?.byteLength ?? 0,
		journalBytes: state.journalStats.totalBytes,
		journalEntries: state.journalStats.entryCount,
	};
}

function row(label: string, r: ColdLoad): void {
	console.log(
		label.padEnd(26) +
		((r.snapshotBytes / 1048576).toFixed(1) + "MB").padStart(8) +
		String(r.journalEntries).padStart(7) +
		((r.journalBytes / 1024).toFixed(0) + "KB").padStart(9) +
		r.loadStateMs.toFixed(1).padStart(10) +
		r.snapshotApplyMs.toFixed(1).padStart(11) +
		r.journalApplyMs.toFixed(1).padStart(11) +
		r.totalMs.toFixed(1).padStart(9) +
		r.peakMiB.toFixed(0).padStart(9)
	);
}

console.log("A. cold load vs vault size (empty journal) — the irreducible wake-up cost\n");
console.log("scenario".padEnd(26) + "snap".padStart(8) + "  ents".padStart(7) + "  jrnl".padStart(9) +
	"  loadMs".padStart(10) + "  snapApply".padStart(11) + "  jrnlApply".padStart(11) + "  total".padStart(9) + "  peakMiB".padStart(9));
for (const mb of [3, 12, 24, 48]) row(`S=${mb}MB, N=0`, coldLoad(mb, 0, false));

console.log("\nB. journal entry count at fixed vault size (S=12MB) — per-entry overhead\n");
console.log("scenario".padEnd(26) + "snap".padStart(8) + "  ents".padStart(7) + "  jrnl".padStart(9) +
	"  loadMs".padStart(10) + "  snapApply".padStart(11) + "  jrnlApply".padStart(11) + "  total".padStart(9) + "  peakMiB".padStart(9));
for (const n of [50, 200, 1000, 4000]) row(`S=12MB, N=${n}`, coldLoad(12, n, false));

console.log("\nC. same content, N entries vs 1 merged entry — the cold-load side of coalescing\n");
console.log("scenario".padEnd(26) + "snap".padStart(8) + "  ents".padStart(7) + "  jrnl".padStart(9) +
	"  loadMs".padStart(10) + "  snapApply".padStart(11) + "  jrnlApply".padStart(11) + "  total".padStart(9) + "  peakMiB".padStart(9));
for (const n of [1000, 4000]) {
	row(`S=12MB, N=${n} separate`, coldLoad(12, n, false));
	row(`S=12MB, N=${n} coalesced`, coldLoad(12, n, true));
}
