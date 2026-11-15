// Overloaded (RFD0013): an object that cannot take a call now says so at once, before it runs the
// call, and the caller gets a typed error saying what is overloaded and when trying again may
// help. A base class: every layer has its kinds, and an app adds its own by subclassing it.
//
// Workers RPC keeps an error's message, not its class or fields, so an Overloaded never crosses
// RPC as an error: the object answers it as plain data (OverloadAnswer), and the client rebuilds
// the class whose `kind` matches, from the ones it knows (do-kit's, then the app's registered).

/**
 * What a client that got an overload may refuse itself until the retry-after, without sending:
 * calls to this shard, this key, or this route. null: nothing; each call is sent.
 */
export type OverloadScope = { readonly shard: string } | { readonly key: string } | { readonly route: string } | null;
/** What the caller is told besides the message: plain values, all of which cross to the caller. */
export type OverloadDetail = Readonly<Record<string, string | number | boolean | null>>;

/** An Overloaded as an object answers it: plain data, over RPC. */
export interface OverloadAnswer {
  readonly kind: string;
  readonly message: string;
  readonly retryAfterMs: number;
  readonly scope: OverloadScope;
  readonly detail: OverloadDetail;
}

export interface OverloadOptions {
  /** When trying again may help, in ms from now. */
  readonly retryAfterMs: number;
  readonly scope?: OverloadScope;
  readonly detail?: OverloadDetail;
}

/**
 * Who refused: an object (it threw, or decided on, an Overloaded), this client (it remembered an
 * object's refusal that covers the call), or the runtime (it shed the call before it ran).
 */
export type OverloadSource = "object" | "client" | "runtime";

/**
 * A call refused because something is overloaded: it was not applied. Throw one (a subclass with
 * a `static kind` of its own, usually) from an object's method before the method writes anything,
 * or inside `transact`, which rolls back: the refusal promises nothing was applied. A subclass
 * keeps this constructor's `(message, options)`, so a client can rebuild it from its answer.
 */
export class Overloaded extends Error {
  /** The kind every instance of the class has: a name in one flat space, unique per class. */
  static readonly kind: string = "overloaded";
  readonly kind: string;
  readonly retryAfterMs: number;
  readonly scope: OverloadScope;
  readonly detail: OverloadDetail;
  /** Set by the client that threw it: "object" until then. */
  source: OverloadSource = "object";

  constructor(message: string, options: OverloadOptions) {
    super(message);
    this.name = new.target.name;
    this.kind = (new.target as typeof Overloaded).kind;
    if (!(Number.isFinite(options.retryAfterMs) && options.retryAfterMs >= 0)) throw new Error(`${this.name}: retryAfterMs must be a number of ms, at least 0`);
    this.retryAfterMs = options.retryAfterMs;
    this.scope = options.scope ?? null;
    this.detail = options.detail ?? {};
  }

  toAnswer(): OverloadAnswer {
    return { kind: this.kind, message: this.message, retryAfterMs: this.retryAfterMs, scope: this.scope, detail: this.detail };
  }

  /** The class's instance for an answer: an answer of a kind no class has keeps its own kind. */
  static fromAnswer(answer: OverloadAnswer, source: OverloadSource): Overloaded {
    const error = new this(answer.message, { retryAfterMs: answer.retryAfterMs, scope: answer.scope, detail: answer.detail });
    if (error.kind !== answer.kind) (error as { kind: string }).kind = answer.kind;
    error.source = source;
    return error;
  }
}

/** A class a client can rebuild an answer as: Overloaded or a subclass. */
export type OverloadClass = Pick<typeof Overloaded, "kind" | "fromAnswer">;

/**
 * A shard is overloaded across many of its keys: a split takes some of them away, so trying again
 * after the retry-after may help.
 */
export class ShardOverloaded extends Overloaded {
  static override readonly kind: string = "shard";
  /** The shard's bits ("" for the root). */
  get bits(): string {
    const scope = this.scope;
    return scope && "shard" in scope ? scope.shard : String(this.detail.bits ?? "");
  }
}

/**
 * One key takes most of its shard's calls: a key is always on one shard, so splitting does not
 * help, and its calls are refused while the shard's other keys are served. Spread the key's load
 * (leased reads for reads, `apply` for updates of one row), or send it less.
 */
export class KeyOverloaded extends Overloaded {
  static override readonly kind: string = "key";
  get key(): string {
    const scope = this.scope;
    return scope && "key" in scope ? scope.key : String(this.detail.key ?? "");
  }
}

/** As KeyOverloaded, for one route (`routeOf`, RFD0006): every key of the route is on one shard. */
export class RouteOverloaded extends Overloaded {
  static override readonly kind: string = "route";
  get route(): string {
    const scope = this.scope;
    return scope && "route" in scope ? scope.route : String(this.detail.route ?? "");
  }
}

/** do-kit's own kinds, which a client always knows. */
export const BUILT_IN_OVERLOADS: readonly OverloadClass[] = [Overloaded, ShardOverloaded, KeyOverloaded, RouteOverloaded];

/** Kinds a client knows: do-kit's, and the app's `overloads`; an app's may not reuse a kind. */
export function overloadRegistry(name: string, extra: readonly OverloadClass[] = []): ReadonlyMap<string, OverloadClass> {
  const byKind = new Map<string, OverloadClass>(BUILT_IN_OVERLOADS.map((c) => [c.kind, c]));
  for (const c of extra) {
    if (typeof c?.kind !== "string" || c.kind === "" || typeof c.fromAnswer !== "function") throw new Error(`sharded ${name}: overloads takes subclasses of Overloaded, each with a static kind`);
    const known = byKind.get(c.kind);
    if (known && known !== c) throw new Error(`sharded ${name}: overload kind ${JSON.stringify(c.kind)} is already ${BUILT_IN_OVERLOADS.includes(known) ? "do-kit's" : "another class's"}; give the class a kind of its own`);
    byKind.set(c.kind, c);
  }
  return byKind;
}

/** `value` as an OverloadAnswer, if it is one: an answer that is not is an UnknownAnswer. */
export function parseOverloadAnswer(value: unknown): OverloadAnswer | null {
  if (!value || typeof value !== "object") return null;
  const a = value as Record<string, unknown>;
  if (typeof a.kind !== "string" || typeof a.message !== "string" || typeof a.retryAfterMs !== "number" || !(a.retryAfterMs >= 0)) return null;
  const scope = parseScope(a.scope);
  if (scope === undefined) return null;
  const detail = a.detail ?? {};
  if (typeof detail !== "object" || detail === null || Array.isArray(detail)) return null;
  for (const v of Object.values(detail)) if (v !== null && !["string", "number", "boolean"].includes(typeof v)) return null;
  return { kind: a.kind, message: a.message, retryAfterMs: a.retryAfterMs, scope, detail: detail as OverloadDetail };
}

/** A scope, null for none, or undefined if `value` is not one. */
function parseScope(value: unknown): OverloadScope | undefined {
  if (value === null || value === undefined) return null;
  if (typeof value !== "object") return undefined;
  const s = value as Record<string, unknown>;
  const keys = Object.keys(s);
  if (keys.length !== 1) return undefined;
  if (typeof s.shard === "string") return { shard: s.shard };
  if (typeof s.key === "string") return { key: s.key };
  if (typeof s.route === "string") return { route: s.route };
  return undefined;
}

/** How long the client treats a shard the runtime shed as overloaded: the runtime says nothing. */
export const RUNTIME_RETRY_AFTER_MS = 1_000;

/** The runtime's own shedding, before the call ran: not a method's error that says the same. */
export function runtimeOverload(bits: string, error: unknown): ShardOverloaded | null {
  const e = error as { overloaded?: unknown; remote?: unknown; message?: unknown } | null;
  // A method's error crosses RPC marked remote: it may have run, whatever it says.
  if (e?.remote === true) return null;
  const message = String(e?.message ?? error).replace(/^(jsg\.)?Error: /, "");
  if (e?.overloaded !== true && !/^Durable Object is overloaded|Too many requests queued/i.test(message)) return null;
  const overload = new ShardOverloaded(`shard ${bits || "root"} is overloaded: the runtime shed the call before it ran (${message.slice(0, 120)})`, { retryAfterMs: RUNTIME_RETRY_AFTER_MS, scope: { shard: bits } });
  overload.source = "runtime";
  (overload as { cause?: unknown }).cause = error;
  return overload;
}
