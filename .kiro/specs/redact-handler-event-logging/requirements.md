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
  key has a whole word of `LOG_COUNTER_KEY_WORDS` (`tokens`, `count`; words are the key split on
  non-alphanumerics and camelCase — e.g. token quotas); numeric values of other sensitive keys
  (e.g. `password`, `otp_token`, `account_password`) SHALL be masked.
- The redaction SHALL recurse into dicts, lists and tuples and SHALL NOT mutate its input.

## R2 — Raw query string and bodies

- WHEN a dict has a `rawQueryString` key with a string value, THE SYSTEM SHALL mask the values of
  the query parameters whose names match `LOG_SENSITIVE_KEY_PARTS` and keep the other parameters
  (and an empty string) intact.
- WHEN a dict has a `body` key with a string value, THE SYSTEM SHALL prepare its logged copy as
  follows, in order: the marker, when the same dict has a truthy `isBase64Encoded`, a sibling
  `headers` dict (any key case) carries a `content-type` (any key case) whose whitespace-trimmed
  value starts with `multipart/`, or the body has the multipart shape on its own (starts with
  `--` after leading whitespace or carries an RFC 2046 preamble before the first boundary — a
  `\n--` line — and contains `content-disposition:` in any case); redacted and re-serialized JSON
  (`ensure_ascii=False`), when the body — stripped of a leading UTF-8 BOM — starts with `{` or `[`
  and parses; the marker, when such a body is too deeply nested to redact or fails to parse;
  redacted like a query string, when the body is form-encoded — a sibling `headers` dict carries a
  whitespace-trimmed `content-type` starting with `application/x-www-form-urlencoded`, or the
  whitespace-stripped body matches the `k=v&k=v` shape (the logged copy is re-encoded from the
  stripped body); unchanged otherwise (e.g. plain text).

## R3 — Extensibility and pass-through guarantees

- THE SYSTEM SHALL read all redaction constants from the module globals at call time, so
  deployments may extend the matching.
- The Processor SHALL receive the original event object and the caller SHALL receive the original
  result object; redaction applies to the logged copies only.
- Log redaction SHALL NOT fail an invocation: when the redacted copy cannot be built (e.g. a
  value too deeply nested to copy), THE SYSTEM SHALL log `LOG_REDACTED_VALUE` and a warning
  naming only the exception type.

## R4 — Where the redacted copies are logged

- THE SYSTEM SHALL build and log the redacted copies of the event and the result only when the
  INFO level is enabled for the module logger; a logger without `isEnabledFor` SHALL be assumed
  to emit INFO.
- The Processor constructor SHALL log a redacted copy of the final config, and `die()` a redacted
  copy of its result, under the same INFO guard.

## Out of scope

- Over-redaction of keys that merely resemble sensitive names (accepted for a log-only helper).
