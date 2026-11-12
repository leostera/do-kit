// The entrypoint a bench Worker is deployed with on teardown (BENCH_TEARDOWN=1): no Durable
// Object classes, so every one of them can be deleted, with its storage, before the Worker is.
export default {
  fetch(): Response {
    return new Response("gone", { status: 410 });
  },
};
