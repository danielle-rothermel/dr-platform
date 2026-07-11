# Exclude DBOS replay payloads from standard export

Standard export allowlists DBOS operational metadata and excludes serialized
workflow/step inputs, outputs, errors, events, streams, and notifications.
Current Whetstone workflow inputs include `database_url`, and replay blobs may
also contain prompts, responses, credentials, or large provider payloads;
typed platform and Whetstone tables remain the analytical sources of truth,
while narrowly scoped DBOS detail is retrieved on demand by the inspector.
