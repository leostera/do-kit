// Work graphs (RFD0005): incremental, parallel execution of a graph of pure units the app defines,
// across many invocations and objects. Define tasks, add units to a graph (typed by the tasks'
// edges), plan it into chunks, and run it: runners pull chunks from a coordinator and write each
// unit's result to a store keyed by a hash of its inputs, which later runs reuse.
export { defineTask, workGraphOf, WorkGraph, type AddOptions, type AnyTask, type Computed, type GraphUnit, type NodeRef, type ResultOfNode, type Task, type TaskContext } from "./graph.js";
export { unitKeyHash, planWorkGraph, unitHash, type Chunk, type PlannedUnit, type PlanOptions, type WorkGraphPlan } from "./plan.js";
export { ComputeFailed, executeChunk, keysToRead, type ChunkBody, type Executed } from "./execute.js";
export { decodeBody, encodeBody, MAX_BODY_BYTES } from "./body.js";
export { decodeStored, encodeStored, MAX_STORED_BYTES, resultKey, valueKey } from "./stored.js";
export { WorkGraphResults } from "./results.js";
export { runnerName, WorkGraphCoordinator, type Finished, type NextAnswer, type RunState, type RunStatus, type StartOptions, type SubmittedChunk } from "./coordinator.js";
export { WorkGraphRunner } from "./runner.js";
export { workGraphResults, workGraphRun, WorkGraphResultsClient, WorkGraphRun } from "./client.js";
