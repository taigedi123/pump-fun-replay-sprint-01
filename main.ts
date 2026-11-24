// Completion-chase paper bot for the pump.fun replay sprint.
// The strategy lives in strategy.ts; this file only wires it to the replay
// protocol's hello handshake, batch loop and order replies.

import { runBot } from './vendor/makefun-bot.ts';
import { createStrategy, DEFAULT_PARAMS } from './strategy.ts';

const strategy = createStrategy(DEFAULT_PARAMS);

await runBot({
  onBatch: (state, batch) => strategy.onBatch(state, batch),
});
