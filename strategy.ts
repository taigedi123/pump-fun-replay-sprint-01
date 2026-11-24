// Completion-chase paper bot for the pump.fun replay sprint.
//
// What the dev data actually shows: a pump.fun curve completes exactly when its
// real SOL reserve reaches 85, and every coin that gets there completes. Buying a
// coin as its real reserve climbs through 30 SOL, with a hard stop and a wide
// take-profit, is a bet on that completion leg: the winners run to +100% and
// beyond, the losers are cut at -20%. Pullbacks, by contrast, are sellers
// dominating — buying them loses. So this bot does one thing: it buys the climb.
//
// Entry is selective on purpose. A crossing counts only when the climb looks
// broad rather than spiky: several distinct buyers behind it, no single wallet
// dominating the flow, the recent net flow strong but not a single huge spike,
// and the coin young — the winners on the dev window are the fast rockets, not
// the slow grinds. Every entry also needs a price that is still rising, so a
// single whale shoving a dead coin over a line does not qualify.
//
// Exits are mechanical: half the position banks at +50%, the rest rides to +100%
// or a trailing stop once it is up, a hard stop caps every loss at -20%, and a
// time limit frees the slot when a coin goes nowhere. A coin is bought at most
// once, so a slot is never tied up twice on the same story.
//
// A small probe trade keeps the run above the competition's guardrail pace
// (round trips on at least 20 coins) when the main signal goes quiet; it is off
// by default because the main signal clears the guardrail on its own.
//
// Deterministic by construction: no clock, no environment, no randomness.
// Iteration only over insertion-ordered maps fed by the batch stream. Amounts are
// SOL as plain numbers until an order is built, where they become lamports.

import type { Batch, BotState, OrderInput, Pubkey } from './vendor/makefun-bot.ts';

const LAMPORTS = 1e9;
/** Curve fee on each side (protocol 95bps + creator 30bps fallback). */
const CURVE_FEE = 0.0125;

export interface Params {
  /** Real-SOL entry tiers, highest first: [threshold, most main positions open]. */
  tiers: Array<{ rq: number; maxHeld: number }>;
  /** Never buy a coin whose real SOL is already above this (the leg is over). */
  maxEntryRq: number;
  /** Recent-flow window (slots) and what it must show. */
  flowWindow: number;
  /** Net buy SOL over the flow window. */
  minFlowSol: number;
  /** Net buy SOL over the flow window, at most: a huge spike is overheated. */
  maxFlowSol: number;
  /** Distinct buyers over the flow window. */
  minBuyers: number;
  /** Largest single buyer's share of the flow window's buy SOL. */
  maxBuyerShare: number;
  /** Oldest age (slots since create) at which a coin may still be bought. */
  maxAge: number;
  /** Price must still be rising over this lookback (slots). */
  riseWindow: number;
  /** SOL per first buy (the 1%-of-real-reserve cap binds first at low reserves). */
  stakeSol: number;
  /** Smallest buy worth sending. */
  minBuySol: number;
  /** Slippage on buys (bps); sells go out wide open so exits always execute. */
  buySlipBps: number;
  /** First take-profit: sell half once the position is worth this over its cost. */
  tp1: number;
  /** Share of the position sold at the first take-profit. */
  tp1Part: number;
  /** Exit whole once the position is worth this over its cost. */
  tp2: number;
  /** Trailing stop arms once value reached cost * trailArm. */
  trailArm: number;
  /** Exit on a fall of this share from the position's best value. */
  trail: number;
  /** Exit when the position is worth this much under its cost. */
  stopLoss: number;
  /** Exit when the real reserve falls below this share of the entry reserve. */
  fadeRqRatio: number;
  /** Exit after holding this many slots. */
  maxHold: number;
  /** Exit when no trade has hit the coin for this many slots. */
  staleSlots: number;
  /** Slots to wait after an exit before the same coin may be bought again. */
  cooldown: number;
  /** Most main positions open at once. */
  maxMain: number;
  /** Most SOL at cost across all positions at once. */
  maxExposureSol: number;
  /** Guardrail pacing: the run must end with round trips on targetCoins coins. */
  targetCoins: number;
  targetFills: number;
  /** Block-time seconds the pace is measured over (the 24h window). */
  paceSecs: number;
  /** SOL per probe buy. */
  probeSol: number;
  /** Most probe positions open at once. */
  maxProbes: number;
}

export const DEFAULT_PARAMS: Params = {
  tiers: [{ rq: 30, maxHeld: 5 }],
  maxEntryRq: 70,
  flowWindow: 30,
  minFlowSol: 0.005,
  maxFlowSol: 35,
  minBuyers: 6,
  maxBuyerShare: 0.35,
  maxAge: 500,
  riseWindow: 60,
  stakeSol: 0.3,
  minBuySol: 0.01,
  buySlipBps: 1500,
  tp1: 1.5,
  tp1Part: 0.5,
  tp2: 2.0,
  trailArm: 1.5,
  trail: 0.25,
  stopLoss: 0.2,
  fadeRqRatio: 0,
  maxHold: 1500,
  staleSlots: 600,
  cooldown: 120,
  maxMain: 5,
  maxExposureSol: 6,
  targetCoins: 25,
  targetFills: 300,
  paceSecs: 86400,
  probeSol: 0.02,
  maxProbes: 0,
};

interface Trade {
  slot: number;
  buy: boolean;
  sol: number;
  user: string;
  price: number;
  rq: number;
}

interface Tape {
  created: number | null;
  trades: Trade[];
  /** Real SOL reserve after the latest trade seen. */
  lastRq: number;
  entered: boolean;
  lastExit: number;
  lastTradeSlot: number;
}

interface Held {
  opened: number;
  peakValue: number;
  invested: number;
  entryRq: number;
  tp1Done: boolean;
  exiting: boolean;
  filled: boolean;
  lastValue: number;
}

export function createStrategy(p: Params = DEFAULT_PARAMS) {
  const tapes = new Map<Pubkey, Tape>();
  const held = new Map<Pubkey, Held>();
  /** Probe round trips in flight, by mint. */
  const probes = new Map<Pubkey, boolean>();
  let myFills = 0;
  const myCoins = new Set<Pubkey>();
  let firstTime: number | null = null;
  let lastTime: number | null = null;
  const keep = Math.max(p.flowWindow, p.riseWindow) + 10;
  let orderNo = 0;

  const tapeOf = (mint: Pubkey): Tape => {
    let t = tapes.get(mint);
    if (t === undefined) {
      t = {
        created: null,
        trades: [],
        lastRq: 0,
        entered: false,
        lastExit: -1e12,
        lastTradeSlot: -1e12,
      };
      tapes.set(mint, t);
    }
    return t;
  };

  /** Net recorded SOL (buys minus sells) over the last `w` slots, plus buyer stats. */
  const flowStats = (
    t: Tape,
    now: number,
    w: number,
  ): { net: number; buySol: number; buyers: number; topShare: number } => {
    let net = 0;
    let buySol = 0;
    const byUser = new Map<string, number>();
    for (let i = t.trades.length - 1; i >= 0; i--) {
      const tr = t.trades[i]!;
      if (tr.slot <= now - w) break;
      if (tr.buy) {
        net += tr.sol;
        buySol += tr.sol;
        byUser.set(tr.user, (byUser.get(tr.user) ?? 0) + tr.sol);
      } else {
        net -= tr.sol;
      }
    }
    let top = 0;
    for (const v of byUser.values()) if (v > top) top = v;
    return { net, buySol, buyers: byUser.size, topShare: buySol > 0 ? top / buySol : 0 };
  };

  /** The price `w` slots ago, from the tape (earliest trade still inside the window). */
  const priceAgo = (t: Tape, now: number, w: number): number => {
    let price = -1;
    for (const tr of t.trades) {
      if (tr.slot <= now - w) continue;
      price = tr.price;
      break;
    }
    return price;
  };

  /** What a holding of `tokens` would fetch now, in SOL, curve and tx fees deducted. */
  const closeValue = (tokens: number, vq: number, vt: number, txSol: number): number => {
    if (tokens <= 0 || vt <= 0) return 0;
    return ((tokens * vq) / (vt + tokens) / LAMPORTS) * (1 - CURVE_FEE) - txSol;
  };

  return {
    onBatch(state: BotState, batch: Batch): OrderInput[] {
      const now = batch.slot;
      const traded = new Set<Pubkey>();
      /** Coins whose real reserve crossed an entry tier in this batch, with the tier. */
      const crossed = new Map<Pubkey, number>();
      for (const f of batch.fills) {
        if (f.status === 'filled' && !f.client_id.startsWith('_')) {
          myFills++;
          myCoins.add(f.mint);
        }
      }
      for (const e of batch.events) {
        if (e.block_time !== null) {
          if (firstTime === null) firstTime = e.block_time;
          if (lastTime === null || e.block_time > lastTime) lastTime = e.block_time;
        }
        if (e.event === 'CreateEvent') {
          tapeOf(e.mint).created = e.slot;
        } else if (e.event === 'TradeEvent') {
          const t = tapeOf(e.mint);
          const sol = Number(e.sol_amount) / LAMPORTS;
          const rq = Number(e.real_quote_reserves) / LAMPORTS;
          const price = Number(e.virtual_quote_reserves) / Number(e.virtual_token_reserves);
          t.trades.push({ slot: e.slot, buy: e.is_buy, sol, user: e.user, price, rq });
          t.lastTradeSlot = e.slot;
          traded.add(e.mint);
          // A tier crossing is the entry trigger: the reserve climbing through a line.
          if (!t.entered && !held.has(e.mint) && !probes.has(e.mint)) {
            for (const tier of p.tiers) {
              if (t.lastRq < tier.rq && rq >= tier.rq && rq <= p.maxEntryRq) {
                const prev = crossed.get(e.mint);
                if (prev === undefined || tier.rq > prev) crossed.set(e.mint, tier.rq);
              }
            }
          }
          t.lastRq = rq;
        }
      }
      for (const mint of traded) {
        const t = tapes.get(mint)!;
        let cut = 0;
        while (cut < t.trades.length && t.trades[cut]!.slot <= now - keep) cut++;
        if (cut > 0) t.trades.splice(0, cut);
      }
      const marks = new Map(batch.marks.map((m) => [m.mint, m] as const));
      const pendingByMint = new Map<Pubkey, number>();
      for (const po of state.pending.values()) {
        pendingByMint.set(po.order.mint, (pendingByMint.get(po.order.mint) ?? 0) + 1);
      }

      const orders: OrderInput[] = [];
      const cap = state.hello.caps.max_orders_per_batch;
      const txSol = Number(state.txFeeLamports()) / LAMPORTS;

      // Exits first: every held position is re-priced and sold on the first trigger.
      for (const [mint, h] of held) {
        const pos = state.positions.get(mint);
        const curve = state.curves.get(mint);
        if (pos === undefined) {
          if (!pendingByMint.has(mint)) {
            held.delete(mint);
            const t = tapeOf(mint);
            if (h.filled) t.lastExit = now;
          }
          continue;
        }
        h.filled = true;
        if (curve === undefined || curve.complete) continue;
        const mk = marks.get(mint);
        const vq = mk ? Number(mk.virtual_quote_reserves) : Number(curve.virtual_quote_reserves);
        const vt = mk ? Number(mk.virtual_token_reserves) : Number(curve.virtual_token_reserves);
        const rqSol = (mk ? Number(mk.real_quote_reserves) : Number(curve.real_quote_reserves)) / LAMPORTS;
        const tokens = Number(pos.token_amount);
        const value = closeValue(tokens, vq, vt, txSol);
        const cost = Number(pos.cost_lamports) / LAMPORTS;
        if (value > h.peakValue) h.peakValue = value;
        h.lastValue = value;
        const t = tapeOf(mint);
        let exitAll = false;
        let exitPart = false;
        if (value < cost * (1 - p.stopLoss)) exitAll = true;
        if (!exitAll && value >= cost * p.tp2) exitAll = true;
        if (!exitAll && h.peakValue >= cost * p.trailArm && value < h.peakValue * (1 - p.trail)) exitAll = true;
        if (!exitAll && now - h.opened > p.maxHold) exitAll = true;
        if (!exitAll && now - t.lastTradeSlot > p.staleSlots) exitAll = true;
        if (!exitAll && rqSol < h.entryRq * p.fadeRqRatio) exitAll = true;
        if (!exitAll && !h.tp1Done && value >= cost * p.tp1) exitPart = true;
        if (!exitAll && !exitPart) continue;
        if (pendingByMint.has(mint)) {
          let sellPending = false;
          for (const po of state.pending.values()) {
            if (po.order.mint === mint && po.order.side === 'sell') sellPending = true;
          }
          if (sellPending) continue;
        }
        if (orders.length >= cap) break;
        if (exitAll) {
          h.exiting = true;
          orders.push({ side: 'sell', client_id: `s${++orderNo}`, mint, token_amount: pos.token_amount, max_slippage_bps: 10_000 });
        } else {
          h.tp1Done = true;
          const part = (BigInt(pos.token_amount) * BigInt(Math.round(p.tp1Part * 1000))) / 1000n;
          if (part > 0n) {
            orders.push({ side: 'sell', client_id: `s${++orderNo}`, mint, token_amount: part, max_slippage_bps: 10_000 });
          }
        }
      }

      // Probe round trips: sell the whole probe as soon as its buy has landed.
      for (const [mint, _selling] of probes) {
        const pos = state.positions.get(mint);
        const curve = state.curves.get(mint);
        if (pos === undefined) {
          if (!pendingByMint.has(mint)) probes.delete(mint);
          continue;
        }
        if (curve === undefined || curve.complete || pendingByMint.has(mint)) continue;
        if (orders.length >= cap) break;
        orders.push({ side: 'sell', client_id: `s${++orderNo}`, mint, token_amount: pos.token_amount, max_slippage_bps: 10_000 });
        probes.set(mint, true);
      }

      // Then entries: coins that crossed a tier in this batch, highest tier first.
      let free = Number(state.freeCashLamports()) / LAMPORTS;
      let slotsLeft = state.hello.caps.max_open_positions - state.committedPositionCount();
      let exposure = 0;
      for (const [mint, h] of held) {
        exposure += Math.max(h.invested, Number(state.positions.get(mint)?.cost_lamports ?? 0n) / LAMPORTS);
      }
      const ranked = [...crossed.entries()].sort((a, b) => b[1] - a[1]);
      for (const [mint, tierRq] of ranked) {
        if (orders.length >= cap) break;
        const curve = state.curves.get(mint);
        if (curve === undefined || curve.complete) continue;
        if (pendingByMint.has(mint) || probes.has(mint) || held.has(mint)) continue;
        if (slotsLeft <= 0) continue;
        const tier = p.tiers.find((x) => x.rq === tierRq)!;
        if (held.size >= tier.maxHeld) continue;
        const t = tapes.get(mint)!;
        if (t.entered || now - t.lastExit < p.cooldown) continue;
        const price = Number(curve.virtual_quote_reserves) / Number(curve.virtual_token_reserves);
        const rqSol = Number(curve.real_quote_reserves) / LAMPORTS;
        if (rqSol > p.maxEntryRq) continue;
        // The climb must still be alive and broad: fresh buy flow from several
        // wallets, no single buyer dominating, not one huge spike, still rising.
        const f = flowStats(t, now, p.flowWindow);
        if (f.net < p.minFlowSol) continue;
        if (f.net > p.maxFlowSol) continue;
        if (f.buyers < p.minBuyers) continue;
        if (f.topShare > p.maxBuyerShare) continue;
        if (t.created === null || now - t.created > p.maxAge) continue;
        const ago = priceAgo(t, now, p.riseWindow);
        if (ago > 0 && price < ago) continue;
        const capSol = Math.floor(Number(state.maxBuyLamports(mint))) / LAMPORTS;
        let amount = Math.min(capSol, p.stakeSol, p.maxExposureSol - exposure);
        if (amount > free - txSol) amount = free - txSol;
        if (amount < p.minBuySol) continue;
        const lamports = BigInt(Math.floor(amount * LAMPORTS));
        orders.push({ side: 'buy', client_id: `b${++orderNo}`, mint, quote_lamports: lamports, max_slippage_bps: p.buySlipBps });
        free -= amount + txSol;
        exposure += amount;
        held.set(mint, {
          opened: now,
          peakValue: 0,
          invested: amount,
          entryRq: rqSol,
          tp1Done: false,
          exiting: false,
          filled: false,
          lastValue: 0,
        });
        t.entered = true;
        slotsLeft--;
      }

      // Guardrail pacing: when fills or coins lag the pace, open a tiny probe round
      // trip on a liquid coin that traded recently, and sell it at the next batch.
      if (probes.size < p.maxProbes && slotsLeft > 0 && firstTime !== null && lastTime !== null) {
        const pace = Math.min(1, (lastTime - firstTime) / p.paceSecs);
        const needCoins = myCoins.size < Math.ceil(p.targetCoins * pace);
        const needFills = myFills < Math.ceil(p.targetFills * pace);
        if (needCoins || needFills) {
          const minRq = (p.probeSol * 10_000) / state.hello.caps.max_order_bps_of_real_reserve;
          for (const mint of traded) {
            if (orders.length >= cap || probes.size >= p.maxProbes) break;
            if (held.has(mint) || probes.has(mint) || pendingByMint.has(mint) || state.positions.has(mint)) continue;
            if (needCoins && !needFills && myCoins.has(mint)) continue;
            const curve = state.curves.get(mint);
            if (curve === undefined || curve.complete) continue;
            if (Number(curve.real_quote_reserves) / LAMPORTS < minRq * 1.5) continue;
            if (free < p.probeSol + 2 * txSol) break;
            orders.push({
              side: 'buy',
              client_id: `b${++orderNo}`,
              mint,
              quote_lamports: BigInt(Math.floor(p.probeSol * LAMPORTS)),
              max_slippage_bps: p.buySlipBps,
            });
            probes.set(mint, false);
            free -= p.probeSol + txSol;
          }
        }
      }
      return orders;
    },
  };
}
