# Handler log redaction — Requirements

## Vision

The generated Lambda handler logs the incoming event and the final result at INFO on every
invocation. API Gateway / CloudFront proxy events carry bearer tokens, cookies, API keys and
origin-verify shared-secret headers, so the raw log lines used to leak request credentials into
the log store. The redaction layer masks them in the logged copies, while the Processor keeps
receiving the original event and the caller the original result.

## R1 — Sensitive keys

- WHEN the lowercase form of a dict key contains any substring from `LOG_SENSITIVE_KEY_PARTS`,
  THE SYSTEM SHALL replace its value with `LOG_REDACTED_VALUE` in the logged copy.
- THE SYSTEM SHALL keep `None` and boolean values of sensitive keys as they are.
- THE SYSTEM SHALL keep a numeric value (`int`, `float`, `Decimal`, booleans excepted) when the
  key also matches `LOG_COUNTER_KEY_PARTS` (counter-like keys, e.g. token quotas); numeric values
  of other sensitive keys SHALL be masked.
- The redaction SHALL recurse into dicts, lists and tuples and SHALL NOT mutate its input.

## R2 — Raw query string and bodies

- WHEN a dict has a `rawQueryString` key with a string value, THE SYSTEM SHALL mask the values of
  the query parameters whose names match `LOG_SENSITIVE_KEY_PARTS` and keep the other parameters
  (and an empty string) intact.
- WHEN a dict has a `body` key with a string value and no truthy `isBase64Encoded` in the same
  dict, THE SYSTEM SHALL parse a body that looks like a JSON object or array, redact it
  recursively and re-serialize it for the log; unparsable, non-JSON (e.g. form-encoded) and
  base64-encoded bodies SHALL be logged unchanged.

## R3 — Extensibility and pass-through guarantees

- THE SYSTEM SHALL read all redaction constants from the module globals at call time, so
  deployments may extend the matching.
- The Processor SHALL receive the original event object and the caller SHALL receive the original
  result object; redaction applies to the logged copies only.

## Out of scope

- Config values logged by the Processor constructor (secrets belong in a secrets manager).
- Over-redaction of keys that merely resemble sensitive names (accepted for a log-only helper).
