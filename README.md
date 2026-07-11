# srt-bonding-relay

Standalone SRT bonding ingress relay. It accepts bonded/redundant SRT groups
on one listener socket and forwards the deduplicated MPEG-TS payload to a
downstream SRT target.

## Data path

```text
        encoder / publisher

   leg A: SRT caller + streamid
   leg B: SRT caller + same streamid
              |
              v
   +-----------------------------+
   | srt-bonding-relay listener |
   | SRTO_GROUPCONNECT=1        |
   +-----------------------------+
              |
              | SRT group socket
              | libsrt merges and deduplicates legs
              v
   +-----------------------------+
   | session thread per streamid |
   | reads one MPEG-TS payload   |
   +-----------------------------+
              |
              +----> srt:// output, same streamid
```

## Design

- **Input listener.** A single SRT socket is bound with `SRTO_GROUPCONNECT=1`
  (`mode=listener`, `transtype=live`, `latency=240ms`). This lets an encoder
  connect multiple redundant network legs (e.g. two NICs/uplinks) as one SRT
  *socket group*; SRT itself merges and deduplicates packets across legs
  before the relay ever sees them, so the relay always reads one clean
  MPEG-TS stream per group, not per leg.
- **Accept loop.** The main thread polls the listener with `srt_epoll_uwait`.
  Each accepted connection (the first leg of a new group, or a follow-up leg
  joining an existing group in the background) spawns a session thread
  (`session_main`) tracked in a fixed-size session table
  (`MAX_ACTIVE_SESSIONS` = 256).
- **Streamid-based routing and dedup.** The caller's SRT `streamid` is read
  off the accepted socket and used as the key for a shared per-stream state
  slot. It's also copied verbatim onto the outgoing connection, so one
  listener can multiplex many logical streams to distinct downstream
  destinations/streamids. If a second publisher connects with a streamid
  that's already active, the relay waits up to 5s for the existing session to
  go quiet; if the existing session has had no input for 5s it's treated as
  stale and force-closed so the new publisher can take over, otherwise the
  new connection is rejected as a duplicate.
- **Forwarding.** Each session thread reads deduplicated payload off the
  group socket with `srt_recvmsg2` and writes it downstream with
  `srt_sendmsg2` over the `srt://` output connection. If the downstream SRT
  target is unreachable, the session retries with backoff (1s, 2s, 4s, 8s,
  16s) in the background without dropping the encoder's input connection.
  If the downstream listener explicitly rejects the stream (for example, an
  unknown or unauthorized stream ID), the relay closes the input immediately
  instead of retaining a session that can never publish.
- **Stats collection.** A per-session sampler (`update_stream_srt_counters`,
  rate-limited to once/sec/session) pulls counters from libsrt
  (`srt_bstats`, `srt_group_data`) under a lock and writes them into the
  shared session state. This runs from the session thread itself — the
  status HTTP thread never touches libsrt directly, it only reads the
  already-collected state.
- **Status HTTP server.** A separate thread serves the JSON status snapshot
  described below on its own port, bound to loopback only.
- **Bad-passphrase rejection.** When `passphrase` is configured, the relay
  disables SRT's default `SRTO_ENFORCEDENCRYPTION` on the input listener and
  instead checks `SRTO_KMSTATE` itself right after `srt_accept()`, closing
  and logging (with the peer's real IP) any connection that did not complete
  a secured key exchange, before a session thread is ever spawned for it. See
  [Bad-passphrase logging](#bad-passphrase-logging) below for why this is
  necessary and how to wire it into `fail2ban`.

## Config

The relay accepts either:

- `srt-bonding-relay <config.json>`
- `srt-bonding-relay <srt-input-uri> <srt-output-uri>`

Minimal JSON config:

```json
{
  "input_host": "0.0.0.0",
  "input_port": 10081,
  "output_host": "127.0.0.1",
  "output_port": 10080,
  "status_port": 8081,
  "passphrase": "secret-value"
}
```

Fields:

- `input_host`: bonded SRT listener host/interface
- `input_port`: bonded SRT listener port
- `output_host`: downstream SRT publish host
- `output_port`: downstream SRT publish port
- `status_port`: local HTTP status endpoint port
- `passphrase`: SRT passphrase applied to the input and to the `srt://` output.
  Use `""` to disable encryption.

The relay is streamid-agnostic. It accepts any incoming `streamid` and copies it
to the downstream SRT publish socket automatically.

All config fields are required.

## Bad-passphrase logging

If `passphrase` is set, every rejected connection attempt is logged to
stderr in a fixed, greppable format:

```text
Rejected connection (bad passphrase) from 203.0.113.7:51234
```

This is also surfaced as `lastError` in the [status API](#status--stats-http-api).

```text
passphrase configured
        |
        v
disable SRT's pre-accept enforced-encryption rejection
        |
        v
srt_accept() returns a socket with the peer address
        |
        v
read SRTO_KMSTATE
        |
        +--> SRT_KM_S_SECURED  ----> start session thread
        |
        +--> anything else     ----> log peer IP and close socket
```

**Why this needs special handling.** By default SRT's own
`SRTO_ENFORCEDENCRYPTION` (on by default, and not something the relay used
to override) rejects a bad-passphrase handshake *inside the SRT handshake
itself* — `srt_accept()` simply never returns that connection, so the
application never learns the peer's address and can't log anything useful.
When a passphrase is configured, the relay now turns
`SRTO_ENFORCEDENCRYPTION` off on the input listener and instead checks
`SRTO_KMSTATE` explicitly right after accepting: unless it comes back
`SRT_KM_S_SECURED`, the connection is closed immediately — no session
thread is spawned and no payload is ever read — and the peer's address is
logged.

This is not a weaker security posture than the SRT default: the socket is
torn down before any data path is touched, and the peer never gets access
to a decrypted stream. The only difference is *where* the rejection happens,
which is what makes it observable.

**fail2ban filter example** (`/etc/fail2ban/filter.d/srt-bonding-relay.conf`):

```ini
[Definition]
failregex = ^Rejected connection \(bad passphrase\) from <HOST>:\d+$
ignoreregex =
```

Point the corresponding jail's `logpath` at wherever the relay's stderr is
captured (a redirected log file, or `journalctl` output if run under
systemd).

## Status / Stats HTTP API

The status thread listens on `127.0.0.1:<status_port>` (loopback only, not
reachable from other hosts) and returns a JSON snapshot of every active or
recently-active stream on any request, e.g.:

```bash
curl http://127.0.0.1:8081/status
```

```text
status HTTP thread
        |
        v
copies in-memory session state
        |
        +--> never calls libsrt directly
        |
        v
JSON response

session threads
        |
        +--> sample srt_bstats() / srt_group_data() at most once per second
        |
        v
shared session state
```

Top-level fields:

- `pid`, `startedAtMs`, `updatedAtMs`: process identity and snapshot timing
- `lastError`: the most recent process-level error, if any
- `activeStreamIds`: streamids with a currently connected input
- `streamStates`: one entry per known stream (see below); entries can briefly
  outlive `activeStreamIds` while a session is tearing down

Each `streamStates[]` entry:

- `streamId`, `inputActive`, `outputConnected`, `retryFailures`
- `forwardedPackets`, `forwardedBytes`, `lastPacketAt`, `lastInputPacketAt`:
  relay-tracked forwarding progress
- `recvPacketsTotal`, `recvUniquePacketsTotal`, `recvLossTotal`,
  `recvDropTotal`, `retransTotal`: **group-level** input counters, i.e. SRT's
  own combined/deduplicated accounting across all bonded legs (`srt_bstats`
  on the group socket). The relay does not sum these itself.
- `inputRttMs`: RTT reported for the input group socket as a whole
- `outputRttMs`, `outputSentPacketsTotal`, `outputSendLossTotal`,
  `outputSendDropTotal`, `outputRetransTotal`: RTT and send-side counters for
  the single downstream connection (output is never bonded)
- `legs`: **per-leg** detail for the bonded input — see below
- `lastErrorAt`, `lastError`: most recent per-stream error, if any

`legs[]` is one entry per individual bonded connection currently in (or
recently in) the input group, read directly from `srt_group_data()` +
`srt_bstats()` on each member socket — this is real per-leg telemetry from
libsrt, not something the relay derives or combines:

- `ip`, `port`: the leg's peer (encoder-side) address, as seen by the relay
- `state`: `pending` | `idle` | `running` | `broken`
- `rttMs`, `recvPacketsTotal`, `recvUniquePacketsTotal`, `recvLossTotal`,
  `recvDropTotal`, `retransTotal`: stats for that leg only

Example response for a stream bonded over two legs:

```json
{
  "pid": 12345,
  "startedAtMs": 1783569708552,
  "updatedAtMs": 1783569763437,
  "lastError": null,
  "activeStreamIds": ["camera1/main"],
  "streamStates": [
    {
      "streamId": "camera1/main",
      "inputActive": true,
      "outputConnected": true,
      "retryFailures": 0,
      "forwardedPackets": 28,
      "forwardedBytes": 36848,
      "lastPacketAt": 1783569742530,
      "lastInputPacketAt": 1783569742529,
      "recvPacketsTotal": null,
      "recvUniquePacketsTotal": 22,
      "recvLossTotal": 0,
      "recvDropTotal": 0,
      "retransTotal": 0,
      "inputRttMs": null,
      "outputRttMs": 0.043,
      "outputSentPacketsTotal": 21,
      "outputSendLossTotal": 0,
      "outputSendDropTotal": 0,
      "outputRetransTotal": 0,
      "legs": [
        {
          "ip": "10.0.1.20",
          "port": 51072,
          "state": "running",
          "rttMs": 24.6,
          "recvPacketsTotal": 26,
          "recvUniquePacketsTotal": 26,
          "recvLossTotal": 0,
          "recvDropTotal": 0,
          "retransTotal": 0
        },
        {
          "ip": "10.0.2.31",
          "port": 39356,
          "state": "running",
          "rttMs": 31.2,
          "recvPacketsTotal": 25,
          "recvUniquePacketsTotal": 25,
          "recvLossTotal": 1,
          "recvDropTotal": 0,
          "retransTotal": 0
        }
      ],
      "lastErrorAt": 0,
      "lastError": null
    }
  ]
}
```

Note `recvPacketsTotal` (group-level, top of the stream entry) can be `null`:
for a bonded group socket SRT doesn't always populate that particular
counter, so it's only reported when SRT marks it valid; `recvUniquePacketsTotal`
is the reliable dedup'd total to use for group-level throughput.

## Versioning

The relay version is stored in the repo's `VERSION` file and embedded into the
binary at build time.

## Local Build

```bash
bash scripts/build-local.sh
```

Check the embedded version:

```bash
./objs/srt-bonding-relay --version
```

Run it:

```bash
bash scripts/run.sh
```

Smoke test it locally:

```bash
bash scripts/smoke-test.sh
```

Format C sources:

```bash
bash scripts/format.sh
```

## GitHub Releases

Build the release asset:

```bash
bash scripts/build-release.sh
```

That produces:

```text
build/srt-bonding-relay-linux-x86_64.tar.gz
```

Publish it to GitHub Releases, for example:

```bash
gh release create "$(cat VERSION)" build/srt-bonding-relay-linux-x86_64.tar.gz \
  --title "srt-bonding-relay $(cat VERSION)" \
  --notes "Initial standalone release"
```

Before a release, update `VERSION` first. The binary version is embedded at
build time, so changing `VERSION` after the asset is built will not update the
already-built archive.

If the release already exists:

```bash
gh release upload "$(cat VERSION)" build/srt-bonding-relay-linux-x86_64.tar.gz --clobber
```

## CI

This repo includes a GitHub Actions workflow that:

- builds the relay with Docker
- checks C formatting with `clang-format`
- checks the usage path
- starts the relay with `srt-bonding-relay.json`
- verifies the local HTTP status endpoint responds
