# RFDs

Requests for Discussion: the design decisions of do-kit, one numbered document each. Start a new one from [RFD0000-template.md](RFD0000-template.md), with the next free number.

do-kit is the smallest set of layers that lets an app pick what fits it. A layer adds one capability
on top of the one below and stops there; it is not a database. An RFD that adds to a layer says why
that capability belongs in it rather than in a layer above, or in the app.

| RFD | Title | Status |
|---|---|---|
| [RFD0001](RFD0001-sharded-object.md) | Sharded objects | Implemented (snapshot) |
| [RFD0002](RFD0002-sharded-table.md) | Sharded tables | Implemented (snapshot) |
| [RFD0003](RFD0003-indexed-table.md) | Indexed tables: global indexes across shards | Draft (phase 1 built) |
| [RFD0004](RFD0004-leased-reads.md) | Leased reads for hot keys | Draft |
