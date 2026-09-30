# Handler log redaction — Design

## Approach

A single recursive helper, `_redact_for_logging(data)`, produces the log copy of the event and of
the result. The generated handler logs the redacted copies and passes the original objects
through untouched — the helper never mutates its input, so the Processor contract is unchanged.

## Key sensitivity

- A dict key matches when its lowercase form contains any substring of `LOG_SENSITIVE_KEY_PARTS`
  (authorization headers, cookies, tokens, secrets, passwords, credentials, signatures, ...).
- Values of sensitive keys: `None` and booleans are kept; numbers (`int`, `float`, `Decimal`) are
  kept only when the key also matches `LOG_COUNTER_KEY_PARTS` (`tokens`, `count`) — counters are
  useful in logs and carry no secret; everything else is masked.
- All constants are read from the module globals at call time: reassigning them extends or tunes
  the matching per deployment.

## Raw query string and bodies

- `rawQueryString` (HTTP API v2 / Function URL events): parsed with
  `urllib.parse.parse_qsl(keep_blank_values=True)`, parameter names matched like dict keys,
  sensitive values replaced with the marker, re-encoded with `urlencode(safe='*')` so the marker
  stays readable.
- `body`: unless the same dict carries a truthy `isBase64Encoded`, a string whose stripped form
  starts with `{` or `[` is parsed with `json.loads`, redacted recursively and re-serialized with
  `json.dumps`; a parse failure or any other string is returned unchanged.

## Trade-offs

- Substring matching over-redacts look-alike keys (`cookie_consent`, `signature_version`);
  accepted for a log-only helper.
- Non-JSON string bodies (e.g. form-encoded) are logged verbatim by design: without a content
  type, masking arbitrary encodings reliably is not possible.
- The re-serialized JSON body is a log copy only — key order is preserved but whitespace differs
  from the original.

## Testing

Unit tests cover proxy events (v1 and v2), mixed-case and non-string keys, containers and
scalars, the `None`/bool/counter numeric rules, the raw query string and every body path (JSON
object, JSON array, invalid JSON, non-JSON, base64 flag), plus handler-level assertions that no
secret value reaches any `logger.info` call while the Processor and the caller keep the original
event and result objects.
