# Exclude DBOS replay payloads from standard export

Standard export allowlists DBOS operational metadata and excludes serialized
workflow/step inputs, outputs, errors, events, streams, and notifications.
Whetstone removes `database_url` and every other secret from workflow arguments
and resolves credentials from process configuration inside execution; replay
blobs may still contain prompts, responses, or large provider payloads. Typed
platform and Whetstone tables remain the analytical sources of truth. Normal
DBOS workflow inspection explicitly disables input/output loading. Because
DBOSClient 2.26.0 cannot disable step-output loading, standard step timelines
use a version-pinned allowlisted system-schema reader that excludes input,
output, error, and serialization columns and contract-tests that no payload is
deserialized. Any future payload debugger is a separately guarded and redacted
surface.
