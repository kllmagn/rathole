# TCP pool recovery (infra.1)

This fork fixes visitor head-of-line blocking and lost data-channel reserve in
`0.6.0-beta.1-infra.1`. It does not change TOML configuration, protocol version,
serialized messages, or TCP/Noise transport framing. No watchdog or process
restart is involved. The server can replenish requests even when an older client
exhausts its data-channel connection retries.

## Defaults and limits

| Setting | Default | Scope |
| --- | --- | --- |
| Visitor startup deadline | 30 seconds from accept | Queue wait, StartForwardTcp write/flush, and optional PROXY header write/flush together |
| Visitor admission limit | 128 per TCP service | Listener queue, pool queue, and startup tasks combined |
| Concurrent startup tasks | 8 per TCP service | A blocked channel does not block other startup tasks |
| Idle data-channel reserve | 8 per TCP service | Excess late/duplicate channels are closed |
| Outstanding request expiry | 30 seconds | Missing channels are requested again, even without visitors |
| Pending creation commands | 8 per TCP service; 2048 for UDP/Unix | Bounded control queue; full TCP queue coalesces retry demand without enqueueing duplicates |
| Control command write deadline | 10 seconds | Entire command/heartbeat write and flush; shutdown interrupts it; cancellation closes the connection |
| Queue/reserve maintenance | 100 milliseconds | Expired visitors and observed socket errors are pruned; missed ticks are skipped |
| Client channel creation deadline | 10 seconds | Entire transport connect/retry/handshake and DataChannelHello write/flush, not the forwarded session |
| Client TCP backend connection deadline | 10 seconds | Backend connect only, independently of data-channel creation and established copying |

These are internal constants, not new config keys. A full startup queue rejects
new sockets immediately and logs `Rejecting TCP visitor: startup queue full`.
Expired startup sockets close instead of waiting forever. Startup failures retry
the same visitor with a different channel, keeping its original deadline. Each
consumed channel creates a reserve deficit independently of that visitor's fate.

Once command/header startup succeeds, forwarding has **no session deadline**.
The server deadline cannot prove the client backend is connected: the existing
wire protocol has no backend-ready acknowledgement. Established tunnels retain
the upstream bidirectional-copy half-close behavior. The 128-slot admission cap
ends when the server delivers the command/header; it does not count client
backend attempts or established sessions. The upgraded client independently
expires each stalled backend connection after ten seconds. This bounds attempt
lifetime, not aggregate backend concurrency or total established-session count.

TCP FIN alone does not distinguish a client that closed its socket from one that
half-closed its write side while awaiting a response. Queue pruning therefore
uses socket errors (including observed resets), not EOF or destructive reads.
FIN-only visitors and resets not yet visible to the OS remain until the startup
deadline. Buffered visitor bytes remain untouched. This also preserves an empty
write-half-close used by server-first protocols.

The reserve manager tracks at most eight outstanding requests and retries missing
slots after expiry. Its physical control queue also holds at most eight pending
creation commands, plus one command currently being written. A full queue
coalesces TCP retry demand without repeated full-queue warnings. Control writes
and flushes have a ten-second deadline and are shutdown-aware. If a partially
written command is cancelled, the whole control connection is closed; its prefix
is never resumed or followed by another command. The protocol has no request IDs, failure acknowledgements, or
cancellation messages; delayed arrivals can satisfy a newer slot. Late surplus
is discarded, so cached channels stay bounded. The upgraded client imposes a
10-second creation deadline to prevent a stalled connect from keeping an orphan
creation task indefinitely, and a separate ten-second deadline for TCP backend
connection. Older clients do not gain those client-side bounds merely by
upgrading the server. UDP and Unix socket pool scheduling and wire commands are
unchanged; their creation-command buffers now use the existing 2048-channel
capacity with backpressure, and share shutdown-aware bounded control writes.

## Validation and failure recovery

Use Rust 1.92.0 as pinned in `rust-toolchain.toml`. Ensure rustup shims precede any
system/Homebrew Rust installation:

```sh
export PATH="$HOME/.cargo/bin:$PATH"
cargo test --no-default-features --features server,client,noise --lib
cargo test --no-default-features --features server,client,noise --test integration_test -- --test-threads=1
cargo check --no-default-features --features server,client,noise
```

Focused pool tests use loopback TCP visitors and in-memory data streams to
reproduce blocked startup writes, missing channels, explicit overflow rejection,
reset pruning, broken-channel retry, shutdown, and FIN preservation. Deadline and
session-lifetime tests deliberately take about 32 seconds in parallel. The
client regression uses virtual time and a connector that never completes.

## Review regression RED/GREEN on Linux

No local cargo build was run for the review fixes. On a freshly synchronized
Linux snapshot with Rust 1.92 and Git installed, run:

```sh
sh scripts/test-tcp-review-red.sh > /tmp/rathole-review-red.log 2>&1
CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 \
  cargo test -j2 --locked --no-default-features --features embedded --lib
cargo test -j2 --locked --no-default-features --features embedded --test integration_test -- --test-threads=1
cargo check -j2 --locked --no-default-features --features embedded
```

The RED script copies source to a disposable directory, applies the documented
counterfactual `tcp-pool-review-red.patch` there, and runs the new request-queue,
partial-control-write, and stalled-backend regressions. It keeps the test seams
but restores the previous unbounded queue/write/connect behavior. It requires
actual test assertion failures: compilation and environmental failures do not
count as RED. The original source is not patched; dependency build output may
reuse `CARGO_TARGET_DIR`. GREEN must then run against the untouched final source.
The backend session-lifetime regression advances virtual time beyond sixty
seconds and still transfers request/response data. The queue regression holds a
creation command out of the receiver while four reserve expiry cycles pass;
separate control regressions verify partial-write shutdown and deadline closure
before the first thirty-second reserve expiry. A late-arrival regression verifies
surplus data channels close while the eight-channel cache remains usable.

For an operational recovery drill, interrupt client-to-server data-channel
creation while keeping the control channel alive. New visitors must close after
the startup deadline. The server must continue requesting missing reserve slots
without requiring a new visitor. Restore connectivity; a fresh visitor must
forward without restarting either process. Repeat with both TCP and Noise, and
verify that a session already forwarding survives more than 30 seconds. This
network fault drill is separate from unit-test verification and should be run on
the target installations by the deployment owner.

If a test fails, preserve its output, rerun the single named test with
`--nocapture`, and check listener availability and the selected Rust toolchain.
Do not increase deadlines to hide a regression. The integration suite uses fixed
ports and `/tmp/rathole_integration_test`; avoid parallel competing runs. A
32-second missing-reserve failure means maintenance is not replenishing; a
one-second healthy-start failure means a stalled channel is blocking unrelated
visitors. Keep failed tests failing until the causal fix is verified.

Production builds for both Linux x86_64 and aarch64 use
`--no-default-features --features embedded`: this enables server, client, Noise,
and hot reload without TLS dependencies. All current live installations use
Noise; the same build also supports TCP transport. Cross-build,
mixed-version runtime verification, packaging, deployment, and infrastructure
documentation belong to the deployment owner; local tests do not establish them.

When local disk space is constrained, avoid release/cross builds and use
`CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0` for
focused local tests. An `ENOSPC`/`No space left on device` compiler failure is an
environmental failure, not a passing test or evidence of a code regression.
Only this repository's generated `target/` may be cleaned; unrelated caches
must remain untouched. Native Linux musl builds/tests run on the target hosts.

The review RED script uses a separate `target/review-red` build directory.
Do not share RED artifacts with GREEN: Cargo may otherwise reuse the faulty
temporary copy's test binary when the final source has older timestamps.
