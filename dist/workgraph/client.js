// The Worker side of work graphs (RFD0005): submitting a plan as a run, starting its runners,
// watching it, and reading results, typed by the graph's nodes.
import { sharded } from "../sharded/client.js";
import { encodeBody, MAX_BODY_BYTES } from "./body.js";
import { decodeStored, resultKey } from "./stored.js";
/** Chunk bytes per submit call (RPC arguments are limited to 32 MiB). */
const SUBMIT_BATCH_BYTES = 8 * 1024 * 1024;
export class WorkGraphRun {
    coordinators;
    name;
    constructor(coordinators, name) {
        this.coordinators = coordinators;
        this.name = name;
    }
    get coordinator() {
        return this.coordinators.get(this.coordinators.idFromName(this.name));
    }
    /**
     * Submits the plan's chunks and seals the run. Submitting again is harmless. Each chunk's body
     * (its units' inputs, mostly) must encode to at most MAX_BODY_BYTES (16 MiB).
     */
    async submit(plan) {
        let batch = [];
        let bytes = 0;
        for (const chunk of plan.chunks) {
            const body = encodeBody(chunk);
            if (body.byteLength > MAX_BODY_BYTES)
                throw new Error(`chunk ${chunk.id}'s inputs are ${body.byteLength} bytes encoded, over ${MAX_BODY_BYTES}: put bulk inputs in your own storage, and a pointer to them in the input`);
            if (batch.length > 0 && bytes + body.byteLength > SUBMIT_BATCH_BYTES) {
                await this.coordinator.graphSubmit(this.name, batch);
                batch = [];
                bytes = 0;
            }
            batch.push({ id: chunk.id, round: chunk.round, cost: chunk.cost, needs: chunk.needs, body });
            bytes += body.byteLength;
        }
        if (batch.length > 0)
            await this.coordinator.graphSubmit(this.name, batch);
        return (await this.coordinator.graphSeal(this.name));
    }
    /** Starts `runners` runners. */
    async start(options) {
        return (await this.coordinator.graphStart(this.name, options));
    }
    async status() {
        return (await this.coordinator.graphStatus());
    }
}
/** A run of a work graph, by name: one coordinator object. */
export function workGraphRun(coordinators, name) {
    return new WorkGraphRun(coordinators, name);
}
export class WorkGraphResultsClient {
    client;
    constructor(namespace, name, options = {}) {
        this.client = sharded(namespace, name, options);
    }
    /** A node's result, if it is stored. */
    async get(plan, node) {
        return (await this.many(plan, [node]))[0];
    }
    /** Nodes' results, in order (undefined where none is stored). */
    async many(plan, nodes) {
        const keys = nodes.map((n) => resultKey(plan.hashOf(n)));
        const out = new Array(nodes.length).fill(undefined);
        for (const shard of await this.client.call(keys, "graphGet", null))
            shard.positions.forEach((p, j) => shard.result[j] !== null && (out[p] = decodeStored(shard.result[j])));
        return out;
    }
}
/** The results store named `name`: runs that write to it share their results. */
export function workGraphResults(namespace, name, options = {}) {
    return new WorkGraphResultsClient(namespace, name, options);
}
//# sourceMappingURL=client.js.map