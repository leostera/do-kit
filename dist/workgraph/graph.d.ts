declare const brand: unique symbol;
/** What a task computes for one unit: a result to keep, and a value for its children. */
export interface Computed<R, V> {
    readonly result: R;
    /** For the unit's children, if it has any (and for a chunk cut below it, stored). */
    readonly value: V;
}
/** What a task's compute is given besides its inputs. */
export interface TaskContext<Env> {
    readonly env: Env;
}
/**
 * A kind of unit. `P` is the value of the parent it transforms (`null`: it has none), `A` names
 * the results it reads from the units it runs after.
 */
export interface Task<Env, Name extends string, I, P, A extends Record<string, unknown>, R, V> {
    readonly name: Name;
    /** Goes into every hash: changing it invalidates this task's results. */
    readonly version: string;
    compute(input: I, parent: P, after: A, ctx: TaskContext<Env>): Computed<R, V> | Promise<Computed<R, V>>;
}
export type AnyTask<Env = any> = Task<Env, string, any, any, any, any, any>;
type InputOf<T> = T extends Task<any, any, infer I, any, any, any, any> ? I : never;
type ParentOf<T> = T extends Task<any, any, any, infer P, any, any, any> ? P : never;
type AfterOf<T> = T extends Task<any, any, any, any, infer A, any, any> ? A : never;
type ResultOf<T> = T extends Task<any, any, any, any, any, infer R, any> ? R : never;
type ValueOf<T> = T extends Task<any, any, any, any, any, any, infer V> ? V : never;
/** A unit in a graph: its key, and (in its type) its task's result and value. */
export interface NodeRef<R, V> {
    readonly key: string;
    readonly task: string;
    readonly [brand]?: {
        readonly result: R;
        readonly value: V;
    };
}
/** The result type of a node. */
export type ResultOfNode<N> = N extends NodeRef<infer R, unknown> ? R : never;
/** How a unit of task T joins the graph: its cost, and the edges its task takes. */
export type AddOptions<T> = {
    readonly cost: number;
} & ([ParentOf<T>] extends [null] ? {
    readonly parent?: never;
} : {
    readonly parent: NodeRef<unknown, ParentOf<T>>;
}) & ([keyof AfterOf<T>] extends [never] ? {
    readonly after?: never;
} : {
    readonly after: {
        readonly [K in keyof AfterOf<T>]: NodeRef<AfterOf<T>[K], unknown>;
    };
});
/**
 * Defines a task. Its types come from `compute`'s parameters: annotate them, and use `null` for
 * the parent of a task with none, and `{}` for after units it does not read.
 */
export declare function defineTask<Env, const Name extends string, I, P, A extends Record<string, unknown>, R, V>(task: Task<Env, Name, I, P, A, R, V>): Task<Env, Name, I, P, A, R, V>;
/** A unit as the planner sees it: untyped, with its task's name and version. */
export interface GraphUnit {
    readonly key: string;
    readonly task: string;
    readonly version: string;
    readonly cost: number;
    readonly input: unknown;
    readonly parent: string | null;
    /** By the name the task reads each result under. */
    readonly after: Readonly<Record<string, string>>;
}
/** A graph being built from tasks of the set `Tasks`. */
export declare class WorkGraph<Tasks extends Record<string, AnyTask>> {
    readonly tasks: Tasks;
    readonly units: GraphUnit[];
    private readonly keys;
    constructor(tasks: Tasks);
    /** Adds a unit of `task`, keyed `key`: parent and after units must already be in the graph. */
    add<T extends Tasks[keyof Tasks]>(task: T, key: string, input: InputOf<T>, options: AddOptions<T>): NodeRef<ResultOf<T>, ValueOf<T>>;
}
/** Starts a graph of units of these tasks. */
export declare function workGraphOf<Tasks extends Record<string, AnyTask>>(tasks: Tasks): WorkGraph<Tasks>;
export {};
//# sourceMappingURL=graph.d.ts.map