# ADR 0024: signed bundle integrity

Promoted bundles carry a `dr-platform.bundle-integrity.v1` canonical payload,
Ed25519 key id and signature. The payload binds destination, bundle key/id,
snapshot, source-coordinate digest, member schema/table, ordered columns,
unique key, logical checksum, count, and destination-native physical-digest
algorithm. Signatures cover `dr-platform.bundle-integrity.v1\0` followed by
canonical JSON. `BundleIntegritySigner` is injected; `OpenSslEd25519Signer`
keeps PEM private keys outside publication state.

The source-coordinate digest is SHA-256 of `_canonical` JSON for the parsed
`source_coordinates_json` array: source-coordinate objects retain their stored
array order, object keys are sorted, and datetimes use their JSON ISO-8601
form. Readers parse that JSON as `SourceCoordinate`, re-canonicalize it, and
derive manifest source families from the authenticated `source_id` prefixes;
the unsigned manifest may not supply independent provenance.
Local DuckDB bundle manifests do not persist source coordinates or source
families, so their existing signed empty coordinate digest has no corresponding
local provenance field to validate.

Migration adds nullable signed fields, physically validates and signs every
current, retained, and active-pinned bundle under the publication fence, then
rejects promotion without a signer. It must abort rather than strand a
protected bundle if any member is invalid. Old public keys remain configured
in readers until their bundles age out and pins are released.

Reader access is defense in depth: grant only physical-member `SELECT` and the
minimal pin operations. This protects against metadata forgery and ordinary
destination DML/DDL principals, not a compromised signing key, malicious
database owner able to lie about snapshots/query results, or a compromised
local OS account controlling both DuckDB and the signer key.
