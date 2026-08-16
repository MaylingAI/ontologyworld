export interface SingleFlightState {
	inFlight: Promise<void> | null;
}

export async function runSingleFlight(
	state: SingleFlightState,
	task: () => Promise<void>,
): Promise<void> {
	if (state.inFlight) {
		await state.inFlight;
		return;
	}

	state.inFlight = (async () => {
		await task();
	})();

	try {
		await state.inFlight;
	} finally {
		state.inFlight = null;
	}
}

export interface SerializedState {
	chain: Promise<void>;
}

export async function runSerialized<T>(
	state: SerializedState,
	task: () => Promise<T>,
): Promise<T> {
	const run = state.chain.then(task);
	state.chain = run.then(() => undefined, () => undefined);
	return await run;
}
