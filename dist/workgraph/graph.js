// Work graphs, typed (RFD0005). A task is a pure function from its input, its parent's value and
// the results of the units it runs after, to a result (kept) and a value (passed to its
// children). A graph is built by adding units of tasks; each `add` returns a handle typed by the
// task's result and value, and a unit's parent and after units are checked against what its task
// takes. So what flows along every edge is checked when the graph is written.
/**
 * Defines a task. Its types come from `compute`'s parameters: annotate them, and use `null` for
 * the parent of a task with none, and `{}` for after units it does not read.
 */
export function defineTask(task) {
    if (!task.name || /[\0]/.test(task.name))
        throw new Error("a task needs a name");
    return task;
}
/** A graph being built from tasks of the set `Tasks`. */
export class WorkGraph {
    tasks;
    units = [];
    keys = new Set();
    constructor(tasks) {
        this.tasks = tasks;
        for (const [name, task] of Object.entries(tasks))
            if (task.name !== name)
                throw new Error(`task ${task.name} is registered as ${name}: use its own name`);
    }
    /** Adds a unit of `task`, keyed `key`: parent and after units must already be in the graph. */
    add(task, key, input, options) {
        if (this.tasks[task.name] !== task)
            throw new Error(`task ${task.name} is not in this graph's task set`);
        if (this.keys.has(key))
            throw new Error(`unit ${key} appears twice`);
        const o = options;
        const parent = o.parent?.key ?? null;
        if (parent !== null && !this.keys.has(parent))
            throw new Error(`unit ${key}'s parent ${parent} is not in the graph`);
        const after = {};
        for (const [name, ref] of Object.entries(o.after ?? {})) {
            if (!this.keys.has(ref.key))
                throw new Error(`unit ${key} runs after ${ref.key}, which is not in the graph`);
            after[name] = ref.key;
        }
        this.keys.add(key);
        this.units.push({ key, task: task.name, version: task.version, cost: o.cost, input, parent, after });
        return { key, task: task.name };
    }
}
/** Starts a graph of units of these tasks. */
export function workGraphOf(tasks) {
    return new WorkGraph(tasks);
}
//# sourceMappingURL=graph.js.map