# Signed bundle integrity v2

Platform signs `dr-platform.bundle-integrity.v1` only after each staged member
has a destination-native physical aggregate.  Neon requires the `pgcrypto`
extension and uses `postgres-pgcrypto-row-json-length-framed-sha256-v1`;
DuckDB/MotherDuck use `duckdb-json-length-framed-sha256-v1`.  An unavailable
function is a promotion failure, never a fallback hash.

The signed JSON is constrained JCS: UTF-8, sorted keys, `ensure_ascii=false`,
and no floats or integers outside JavaScript's safe range.  Persist 64-bit
facts as decimal strings in future integrity payload revisions.

`OpenSslEd25519Signer` requires `/usr/bin/openssl` at runtime and invokes it
only with the fixed `pkeyutl -sign -rawin -inkey <operator PEM>` argv.  No key
material is emitted.  Derive the Unitbench key-ring value from the same PEM:

```sh
/usr/bin/openssl pkey -in bundle-ed25519.pem -pubout -outform DER | base64
```

Set `UNITBENCH_BUNDLE_INTEGRITY_PUBLIC_KEYS` to a JSON object keyed by the
Platform signer `key_id`.  During rotation publish both old and new SPKI DER
entries, deploy readers, switch Platform to the new PEM/key id, then remove the
old entry after all retained and pinned bundles have expired.
