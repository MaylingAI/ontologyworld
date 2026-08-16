/**
 * Canonical home for code shared by the Obsidian plugin and the Worker.
 *
 * It lives under server/src/ rather than a repo-root shared/ because
 * build-server-release.mjs copies only paths under server/ into the user's
 * standalone server repo, and server/tsconfig.json pins rootDir to "src" — a
 * root-level shared/ tree would typecheck locally and then be missing from
 * every shipped server zip. The plugin reaches it through the "@shared/*"
 * tsconfig path alias (see the root tsconfig.json), which esbuild honours.
 */

/** Bounded-concurrency parallel map. Results keep input order. */
export async function mapWithConcurrency<T, R>(
	items: readonly T[],
	limit: number,
	worker: (item: T, index: number) => Promise<R>,
): Promise<R[]> {
	if (items.length === 0) return [];

	const normalizedLimit = Math.max(1, Math.min(limit, items.length));
	const results = new Array<R>(items.length);
	let nextIndex = 0;

	async function runWorker(): Promise<void> {
		while (true) {
			const index = nextIndex++;
			if (index >= items.length) return;
			results[index] = await worker(items[index] as T, index);
		}
	}

	await Promise.all(
		Array.from({ length: normalizedLimit }, () => runWorker()),
	);

	return results;
}
