# srt-bonding-relay

Standalone SRT bonding ingress relay. It accepts bonded/redundant SRT groups
on one listener socket and forwards the deduplicated MPEG-TS payload to a
downstream SRT or UDP target.

## Config

The relay accepts either:

- `srt-bonding-relay <config.json>`
- `srt-bonding-relay <input-uri> <output-uri>`

Minimal JSON config:

```json
{
  "input_host": "0.0.0.0",
  "input_port": 10081,
  "output_host": "127.0.0.1",
  "output_port": 10080,
  "status_port": 10082,
  "passphrase": "secret-value"
}
```

Fields:

- `input_host`: bonded SRT listener host/interface
- `input_port`: bonded SRT listener port
- `output_host`: downstream SRT publish host
- `output_port`: downstream SRT publish port
- `status_port`: local HTTP status endpoint port
- `passphrase`: SRT passphrase applied to both input and output. Use `""` to disable encryption.

The relay is streamid-agnostic. It accepts any incoming `streamid` and copies it
to the downstream SRT publish socket automatically.

All config fields are required.

## Local Build

```bash
bash scripts/build-local.sh
```

Run it:

```bash
bash scripts/run.sh
```

Smoke test it locally:

```bash
bash scripts/smoke-test.sh
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
gh release create v1.0.0 build/srt-bonding-relay-linux-x86_64.tar.gz \
  --title "srt-bonding-relay v1.0.0" \
  --notes "Initial standalone release"
```

If the release already exists:

```bash
gh release upload v1.0.0 build/srt-bonding-relay-linux-x86_64.tar.gz --clobber
```

## CI

This repo includes a GitHub Actions workflow that:

- builds the relay with Docker
- checks the usage path
- starts the relay with `srt-bonding-relay.json`
- verifies the local HTTP status endpoint responds
