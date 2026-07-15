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
  target is unreachable or rejects the publish attempt, the session retries
  with backoff (1s, 2s, 4s, 8s, 16s) in the background without dropping the
  encoder's input connection. When libsrt exposes a downstream reject reason,
  it is included in the stream's `lastError`.
- **Stats collection.** A per-session sampler (`update_stream_srt_counters`,
  rate-limited to once/sec/session) pulls counters from libsrt
  (`srt_bstats`, `srt_group_data`) under a lock and writes them into the
  shared session state. This runs from the session thread itself — the
  status HTTP thread never touches libsrt directly, it only reads the
  already-collected state.
- **Status HTTP server.** A separate thread serves the JSON status snapshot
  described below on its own port, bound to loopback only.

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

## Passphrase handling

When `passphrase` is set, the relay configures `SRTO_PASSPHRASE` on the input
listener and output connection, then leaves passphrase validation to libsrt's
default encrypted-handshake handling.

Bad-passphrase callers are rejected by libsrt during the handshake, before
`srt_accept()` returns a connected socket to the relay. That means the relay
does not get a peer address for those failed attempts and does not emit a
relay-owned, fail2ban-ready bad-passphrase log line. Depending on libsrt's
runtime logging, journald may still capture internal SRT error messages for
failed handshakes, but they should be treated as diagnostic noise rather than
a stable security log format.

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

The response body itself is compact (no pretty-printing) since nothing
requires this JSON to be human-formatted — pipe it through `jq`/`python3 -m
json.tool` if you want to read it by eye.

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
- `input`: everything SRT reports about the (possibly bonded) input — see below
- `output`: everything SRT reports about the single downstream connection
  (output is never bonded) — see below
- `lastErrorAt`, `lastError`: most recent per-stream error, if any

`input` fields:

- `recvPacketsTotal`, `recvUniquePacketsTotal`, `recvLossTotal`,
  `recvDropTotal`, `retransTotal`: **group-level** counters, i.e. SRT's own
  combined/deduplicated accounting across all bonded legs (`srt_bstats` on
  the group socket). The relay does not sum these itself. `recvPacketsTotal`
  specifically can be `null`: for a bonded group socket SRT doesn't always
  populate that particular counter, so it's only reported when SRT marks it
  valid — `recvUniquePacketsTotal` is the reliable dedup'd total to use for
  group-level throughput.
- `rttMs`: RTT reported for the input group socket as a whole
- `latencyMs`: negotiated SRT buffering latency for the input, i.e. the
  actual value SRT settled on after the handshake, not just what was
  requested. For a non-bonded input this comes straight from `srt_bstats`
  (`msRcvTsbPdDelay`, the same underlying value `SRTO_LATENCY` reports). A
  bonded group socket's own `srt_bstats` never fills that field in, so for a
  real bonded input this is derived as the max `latencyMs` across `legs[]`
  instead (see below) — no extra libsrt call needed either way.
- `bandwidthMbps`, `recvRateMbps`, `belatedTotal`, `belatedAvgMs`,
  `undecryptTotal`, `reorderDistance`, `rcvBufMs`: estimated capacity, actual
  receive rate, packets dropped for arriving past their delivery deadline
  (count + average lateness), decryption failures, reordering, and
  receive-buffer occupancy (ms) — all read straight from the same
  `srt_bstats` call as the fields above. **Only populated for a non-bonded
  input** (`null` otherwise): like `latencyMs`, a bonded group's own
  `srt_bstats` never fills these in, and there was no reasonable per-leg
  aggregation for most of them (unlike latency, where "max across legs" has
  a clear meaning) — check `legs[]` for the real per-leg numbers on a bonded
  stream instead.
- `legs`: one entry per individual bonded connection currently in (or
  recently in) the input group, read directly from `srt_group_data()` +
  `srt_bstats()` on each member socket — this is real per-leg telemetry from
  libsrt, not something the relay derives or combines:
  - `ip`, `port`: the leg's peer (encoder-side) address, as seen by the relay
  - `state`: `pending` | `idle` | `running` | `broken`
  - `rttMs`, `recvPacketsTotal`, `recvUniquePacketsTotal`, `recvLossTotal`,
    `recvDropTotal`, `retransTotal`: stats for that leg only
  - `latencyMs`: the negotiated latency for that individual leg's own socket
    (`msRcvTsbPdDelay` from the member's own `srt_bstats`, not the group), so
    if an encoder ever negotiates a different value per leg it's visible here
  - `bandwidthMbps`, `recvRateMbps`, `belatedTotal`, `belatedAvgMs`,
    `undecryptTotal`, `reorderDistance`, `rcvBufMs`: the same set of extra
    stats as `input.*` above, but for that leg's own socket specifically —
    reliably populated per leg even though the group-level `input.*`
    versions are not

`output` fields:

- `sentPacketsTotal`, `sendLossTotal`, `sendDropTotal`, `retransTotal`:
  send-side counters for the downstream connection
- `rttMs`: RTT for the downstream connection
- `latencyMs`: negotiated latency on the downstream `srt://` connection the
  relay makes to the output (from `srt_bstats`' `msRcvTsbPdDelay`, same value
  as `input.latencyMs`); the relay's default `build_srt_uri()` requests
  `latency=200` here vs. `latency=240` on the bonded input listener
- `bandwidthMbps`, `sendRateMbps`, `undecryptTotal`, `sndBufMs`: capacity,
  actual send rate, decryption failures, and send-buffer occupancy (ms) —
  the output socket is never a group, so these are populated after the first
  successful output stats sample while connected

Example response for a stream bonded over two legs (reformatted here for
readability — the actual response is one compact line):

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
      "input": {
        "recvPacketsTotal": null,
        "recvUniquePacketsTotal": 22,
        "recvLossTotal": 0,
        "recvDropTotal": 0,
        "retransTotal": 0,
        "rttMs": null,
        "latencyMs": 240,
        "bandwidthMbps": null,
        "recvRateMbps": null,
        "belatedTotal": null,
        "belatedAvgMs": null,
        "undecryptTotal": null,
        "reorderDistance": null,
        "rcvBufMs": null,
        "legs": [
          {
            "ip": "10.0.1.20",
            "port": 51072,
            "state": "running",
            "rttMs": 24.6,
            "latencyMs": 240,
            "recvPacketsTotal": 26,
            "recvUniquePacketsTotal": 26,
            "recvLossTotal": 0,
            "recvDropTotal": 0,
            "retransTotal": 0,
            "bandwidthMbps": 12.3,
            "recvRateMbps": 0.47,
            "belatedTotal": 0,
            "belatedAvgMs": 0.0,
            "undecryptTotal": 0,
            "reorderDistance": 0,
            "rcvBufMs": 221
          },
          {
            "ip": "10.0.2.31",
            "port": 39356,
            "state": "running",
            "rttMs": 31.2,
            "latencyMs": 240,
            "recvPacketsTotal": 25,
            "recvUniquePacketsTotal": 25,
            "recvLossTotal": 1,
            "recvDropTotal": 0,
            "retransTotal": 0,
            "bandwidthMbps": 9.8,
            "recvRateMbps": 0.46,
            "belatedTotal": 2,
            "belatedAvgMs": 6.4,
            "undecryptTotal": 0,
            "reorderDistance": 1,
            "rcvBufMs": 235
          }
        ]
      },
      "output": {
        "sentPacketsTotal": 21,
        "sendLossTotal": 0,
        "sendDropTotal": 0,
        "retransTotal": 0,
        "rttMs": 0.043,
        "latencyMs": 200,
        "bandwidthMbps": 11.8,
        "sendRateMbps": 0.42,
        "undecryptTotal": 0,
        "sndBufMs": 1
      },
      "lastErrorAt": 0,
      "lastError": null
    }
  ]
}
```

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
