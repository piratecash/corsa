# D1 owned resource accounting

The inactive D1 candidate reports the installed intake/local owner, its last
published phase/counter snapshots, operational writer custody, one active round
batch, and local queue occupancy through the overlay resource breakdown and DTO.
These reads do not activate the protocol, sample admission time, change budgets,
or mark construction Ready.

Writer custody belongs to the original intake owner. It includes preoffer,
queued, cancelled-but-still-queued and in-flight items even when their core has
left the current Node registry or their pending row has been released. A private
item claim transfers its mutable wrapper to the sole consumer, or lets a refused
producer detach only its own unclaimed wrapper. The producer never drains the
channel or clears shared work/receipt/plan state. The immutable core header
survives detachment. Consumer release follows write, mandatory Finish and capture
callback return. Gate-discard/drain release after exact cancellation. Writer
cleanup and its signalWriterDone defer complete before writerExited closes.

The writer's counters are independent atomic loads, not one coherent tuple:
counts and bytes can transiently represent different instants. Existing owner
and local snapshots are separate publications. Gauges describe named structural
storage in these scopes, not one simultaneous heap census or reclamation event.
Empty detached wrappers can remain buffered until their core is collected and
are explicitly excluded from operational wrapper bytes.

Each distinct forward work cell can retain its original cut, claim, execution,
plan, success receipts and fixed batch arrays after logical pending release.
Several cells from one round share those objects; different rounds can retain
separate arrays. The per-work named estimate deliberately repeats shared graphs
and excludes allocator/map/GC/TLS/session-pointee overhead. It is reported
separately from the additive storage floor, alongside active scheduler batch
storage and references. This estimate is neither a unique graph count nor a heap
upper bound. Local queue/signing occupancy is saturation, not a claim that a
logical completion freed a referenced cell.

Directed tests cover shared-round estimates, read-only snapshots, a producer
parked before offer through Close/drain and a late refused offer, a losing
producer while the genuine consumer is alive, cancelled queued work, nonoffers,
answer custody after pending release, and completed Finish while a capture
callback still retains the item. Finite Source closure pins the implementation
and the exact metric acquisition/caller/import/type sites; it is not runtime,
compiler allocation or heap proof.

T5 measurement of the actual retained graph, concurrency, launcher/transport and
resource affordability remains open. This change adds no resource threshold,
release approval, capability advertisement or Ready transition.
