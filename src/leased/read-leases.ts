// Read leases (the source's side). One Durable Object holds the copy of some keys everyone reads
// (the source); it leases them to relays (relay-reads.ts), which answer readers from the lease and
// give them sub-leases inside it. A write to a leased key commits at once, but is shown (to reads)
// and acknowledged only once every lease on the key is over, so a read served from a relay or an
// isolate never misses an acknowledged write, and reads never go back.
//
// Time: the holder times its lease from before it asked, less READ_LEASE_MARGIN_MS; the source
// from after it granted, plus the margin. So only clock rates need to agree. Before granting, the
// source stores how far its leases may reach (`horizon`, in steps); a restarted source holds every
// write until then, plus RESTART_SKEW_MS for another machine's clock.
//
// The row reads see while a write is held is in memory; a restarted source has lost it, and shows
// the write. Holders with leases from before the restart still answer with the row from before
// it, so a read through one, after a read of the source, would go back. The source can't tell by
// its clock when those leases are over: it may be on another machine, RESTART_SKEW_MS ahead or
// behind. So the keys with a held write are stored (`HeldKeys`), in the transaction that commits
// the write, and a restarted source answers no read of them (`readableAt`) until their holds plus
// the skew have passed. A read that waits is never wrong.
//
// A named holder's lease (a relay's, by its index) can be asked back: a held write asks each
// holder (`toRevoke`), and a holder that gives it back (`returned`) ends it early, so the write is
// shown once the holders' sub-leases are over, not when their leases would have ended. A named
// holder gets no lease while a write to the key is held: the hold ends with the returns, and a
// lease granted meanwhile would have to be asked back too.

/** How long a read lease lasts. */
export const READ_LEASE_MS = 1000;
/** Clock drift allowed over one lease: the isolate ends it this much early, the source this much late. */
export const READ_LEASE_MARGIN_MS = 150;
/** The holder of leases that can't be asked back (ReadLeases.grant without one). */
const UNNAMED = -1;
/** A lease shorter than this is not worth giving: the holder would ask again at once. */
export const MIN_LEASE_MS = 200;
/** How long before its lease ends a relay renews it (relay-reads.ts). */
export const RENEW_MS = 600;
/** The horizon is stored this far ahead, so a write is needed only every few seconds. */
const HORIZON_STEP_MS = 5000;
/**
 * A key written this recently is not leased to relays: it is being written, and each lease out
 * would hold the next write until it was asked back. Relays read it through instead.
 */
export const WRITTEN_MS = 10_000;
/** How far apart two machines' clocks may be: a restarted source holds this past its horizon. */
const RESTART_SKEW_MS = 1000;

/** A key a write changes, and its row before the write. */
export interface Change<Row> {
  readonly key: string;
  readonly before: Row | undefined;
}

/** A key with a held write, and when the hold ends by the clock of the source that set it. */
export interface HeldKey {
  readonly key: string;
  readonly until: number;
}

/**
 * Where a source keeps its held keys, so they outlive it: its own durable storage, written
 * synchronously. `put` runs inside `holdUntil`, so call `holdUntil` in the transaction that
 * commits the write: the write and its key are then stored together or not at all.
 */
export interface HeldKeys {
  /** Every key stored: read once, when the source starts. */
  load(): readonly HeldKey[];
  /** A key's hold began, or its end changed: replaces the key's entry. */
  put(held: HeldKey): void;
  /** A key's hold is over. */
  delete(key: string): void;
}

export class ReadLeases<Row> {
  /** Until then every write is held: leases granted before a restart may still run. */
  private readonly holdAllUntil: number;
  private horizon: number;
  private readonly storeHorizon: (horizon: number) => void;
  private readonly heldKeys: HeldKeys;
  /**
   * Keys that had a held write when this source last stopped: when their reads may be answered
   * (the stored hold's end plus RESTART_SKEW_MS).
   */
  private readonly unsettled = new Map<string, number>();
  /**
   * The leases out on each key, by holder (a relay's index; UNNAMED for holders that can't be
   * asked back): when each ends by this source's clock (margin included), and whether it was asked back.
   */
  private readonly leases = new Map<string, Map<number, { end: number; revoked: boolean }>>();
  /** Keys with a held write: the row reads see until `until`. Each key is in `heldKeys` too. */
  private readonly held = new Map<string, { row: Row | undefined; until: number }>();
  /** When each key last changed (holdUntil sees every change): not leased to relays for WRITTEN_MS after. */
  private readonly changedAt = new Map<string, number>();

  /**
   * `horizon`: how far leases granted before this run may reach (stored by `storeHorizon`).
   * `heldKeys`: the keys this source held writes on when it last ran, and where it keeps them
   * from now on.
   */
  constructor(horizon: number, storeHorizon: (horizon: number) => void, heldKeys: HeldKeys) {
    this.horizon = horizon;
    this.storeHorizon = storeHorizon;
    this.heldKeys = heldKeys;
    this.holdAllUntil = horizon > 0 ? horizon + RESTART_SKEW_MS : 0;
    const now = Date.now();
    for (const h of heldKeys.load()) {
      if (h.until + RESTART_SKEW_MS > now) this.unsettled.set(h.key, h.until + RESTART_SKEW_MS);
      else heldKeys.delete(h.key);
    }
  }

  /**
   * When reads of `keys` (of every key, without them) may be answered: now, or, after a restart,
   * once the holds the source had on them when it stopped are over on any clock. Wait until then
   * before `visible` and `grant`: this source no longer has the rows from before those writes.
   */
  readableAt(keys?: readonly string[]): number {
    const now = Date.now();
    let at = now;
    for (const key of keys ?? this.unsettled.keys()) {
      const settles = this.unsettled.get(key);
      if (settles !== undefined && settles > now) at = Math.max(at, settles);
    }
    return at;
  }

  /**
   * A lease on `keys` (ms; 0 if none). A held write waits for the leases out now, so a lease
   * granted while one is held ends by then (with the margin), and none is given if that leaves
   * less than MIN_LEASE_MS: the hold is never made longer.
   */
  grant(keys: readonly string[], holder: number = UNNAMED): number {
    const now = Date.now();
    let ms = READ_LEASE_MS;
    for (const key of keys) {
      if (holder !== UNNAMED && now - (this.changedAt.get(key) ?? Number.NEGATIVE_INFINITY) < WRITTEN_MS) return 0;
      const held = this.held.get(key);
      if (held && held.until > now) {
        if (holder !== UNNAMED) return 0;
        ms = Math.min(ms, held.until - now - READ_LEASE_MARGIN_MS);
      }
    }
    if (ms < MIN_LEASE_MS) return 0;
    const end = now + ms + READ_LEASE_MARGIN_MS;
    // Stored before the answer leaves (the output gate holds it until this is durable).
    if (end > this.horizon) {
      this.horizon = now + HORIZON_STEP_MS;
      this.storeHorizon(this.horizon);
    }
    for (const key of keys) {
      let holders = this.leases.get(key);
      if (!holders) this.leases.set(key, (holders = new Map()));
      for (const [h, l] of holders) if (l.end <= now) holders.delete(h);
      holders.set(holder, { end: Math.max(holders.get(holder)?.end ?? 0, end), revoked: false });
    }
    this.sweep(now);
    return ms;
  }

  /**
   * Forgets the keys whose leases are all over, at most once a lease's length: a key leased once
   * and never again would otherwise stay here for as long as the source runs.
   */
  private sweep(now: number): void {
    if (now - this.sweptAt < READ_LEASE_MS) return;
    this.sweptAt = now;
    for (const [key, holders] of this.leases) {
      for (const [h, l] of holders) if (l.end <= now) holders.delete(h);
      if (holders.size === 0) this.leases.delete(key);
    }
    for (const [key, held] of this.held) if (held.until <= now) this.release(key);
    for (const [key, settles] of this.unsettled) {
      if (settles > now) continue;
      this.unsettled.delete(key);
      if (!this.held.has(key)) this.heldKeys.delete(key);
    }
  }

  /**
   * A key's hold is over: forgotten here, and in storage unless reads of the key still wait out a
   * restart (a later restart must wait as long).
   */
  private release(key: string): void {
    this.held.delete(key);
    if ((this.unsettled.get(key) ?? 0) <= Date.now()) this.heldKeys.delete(key);
  }

  /**
   * Sets a key's hold, here and in storage. A hold set after a restart ends no earlier than the
   * restart rule's (`holdAllUntil`), which is past every hold stored before it: replacing a key's
   * stored end never shortens what a later restart waits for.
   */
  private hold(key: string, row: Row | undefined, until: number): void {
    this.held.set(key, { row, until });
    this.heldKeys.put({ key, until });
  }
  private sweptAt = 0;

  /** How many keys have a lease on record (for tests and operators). */
  get leasedKeys(): number {
    return this.leases.size;
  }

  /** What a held write on `key` waits for (ms from now): the latest named and unnamed lease, and the restart hold. For logs. */
  waitsOn(key: string): { named: number; namedOut: number; unnamed: number; restart: number } {
    const now = Date.now();
    let named = 0;
    let namedOut = 0;
    let unnamed = 0;
    for (const [holder, l] of this.leases.get(key) ?? []) {
      if (l.end <= now) continue;
      if (holder === UNNAMED) unnamed = Math.max(unnamed, l.end - now);
      else {
        named = Math.max(named, l.end - now);
        namedOut++;
      }
    }
    return { named, namedOut, unnamed, restart: Math.max(0, this.holdAllUntil - now) };
  }

  /** When the last lease out on `key` ends (0 if none). */
  private leaseEnd(key: string): number {
    let end = 0;
    for (const l of this.leases.get(key)?.values() ?? []) end = Math.max(end, l.end);
    return end;
  }

  /** The named holders of leases on `key` a held write waits for and hasn't asked back yet; each is marked asked. */
  toRevoke(key: string): number[] {
    const now = Date.now();
    const held = this.held.get(key);
    if (!held || held.until <= now) return [];
    const out: number[] = [];
    for (const [holder, l] of this.leases.get(key) ?? []) {
      if (holder === UNNAMED || l.revoked || l.end <= now) continue;
      l.revoked = true;
      out.push(holder);
    }
    return out;
  }

  /**
   * `holder` gave its lease on `key` back: it ends `inMs` from now (when the sub-leases it gave
   * are over, by its clock: plus the margin), or now if none are out, if that is sooner. Returns when a write held on `key` is shown now.
   */
  returned(key: string, holder: number, inMs: number): number {
    const now = Date.now();
    const l = this.leases.get(key)?.get(holder);
    // None out: the relay dropped the lease before it answered, so no clock is involved.
    if (l) l.end = Math.min(l.end, inMs > 0 ? now + inMs + READ_LEASE_MARGIN_MS : now);
    const held = this.held.get(key);
    if (!held) return now;
    const until = Math.min(held.until, Math.max(now, this.leaseEnd(key), this.holdAllUntil));
    // Every lease on the key is over now: nothing is left for a restart to wait for.
    if (until <= now) this.release(key);
    else if (until !== held.until) this.hold(key, held.row, until);
    return until;
  }

  /**
   * Before a write to these keys is shown: when every lease on them is over. If that is later than
   * now, reads keep seeing `before` (each key's row before the write) until then.
   */
  holdUntil(changes: readonly Change<Row>[]): number {
    const now = Date.now();
    for (const c of changes) this.changedAt.set(c.key, now);
    for (const [key, at] of this.changedAt) if (now - at >= WRITTEN_MS) this.changedAt.delete(key);
    let until = now;
    for (const c of changes) until = Math.max(until, this.leaseEnd(c.key), this.holdAllUntil);
    if (until <= now) return now;
    for (const c of changes) {
      const held = this.held.get(c.key);
      // A second write while one is held: reads keep the row from before the first.
      this.hold(c.key, held && held.until > now ? held.row : c.before, Math.max(until, held?.until ?? 0));
    }
    return until;
  }

  /** The row reads see: the one before a held write until it is shown, else the committed one. */
  visible(key: string, committed: Row | undefined): Row | undefined {
    const held = this.held.get(key);
    if (!held) return committed;
    if (held.until > Date.now()) return held.row;
    this.release(key);
    return committed;
  }

  /** When a write held on `key` is shown (now, if none is). */
  shownAt(key: string): number {
    const held = this.held.get(key);
    return held && held.until > Date.now() ? held.until : Date.now();
  }

  /** When the last held write is shown (now or earlier if none is held). */
  heldUntil(): number {
    let until = 0;
    for (const held of this.held.values()) until = Math.max(until, held.until);
    return until;
  }

  /** Whether some write is held: reads (and the digest) see a row from before it. */
  holdsAny(): boolean {
    const now = Date.now();
    for (const held of this.held.values()) if (held.until > now) return true;
    return false;
  }
}
