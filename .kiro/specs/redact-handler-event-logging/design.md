# Handler log redaction — Design

## Approach

A single recursive helper, `_redact_for_logging(data)`, produces the log copy of the event and of
the result. The generated handler logs the redacted copies and passes the original objects
through untouched — the helper never mutates its input, so the Processor contract is unchanged.
Logging must never fail an invocation: `_log_redacted()` guards the two handler log lines — any
failure to build the redacted copy logs the marker and a warning naming only the exception type —
and `_redact_body()` guards the JSON path, logging a value too deeply nested to redact as the
marker instead of raising `RecursionError`.

## Key sensitivity

- A dict key matches when its lowercase form contains any substring of `LOG_SENSITIVE_KEY_PARTS`
  (authorization headers, cookies, tokens, secrets, passwords, credentials, signatures, ...).
- Values of sensitive keys: `None` and booleans are kept; numbers (`int`, `float`, `Decimal`) are
  kept only when the key has a whole word of `LOG_COUNTER_KEY_WORDS` (`tokens`, `count`). Words of
  a key are its lowercase chunks split on any non-alphanumeric character and on camelCase
  boundaries, so `max_tokens`, `input_tokens`, `tokenCount` and `token_count` keep their numbers
  while `account_password`, `service_account_secret`, `password` and `otp_token` do not;
  everything else is masked.
- All constants are read from the module globals at call time: reassigning them extends or tunes
  the matching per deployment.

## Raw query string and bodies

- `rawQueryString` (HTTP API v2 / Function URL events): parsed with
  `urllib.parse.parse_qsl(keep_blank_values=True)`, parameter names matched like dict keys,
  sensitive values replaced with the marker, re-encoded with `urlencode(safe='*')` so the marker
  stays readable.
- `body`, prepared by `_redact_body(body, container)`:
  - a truthy sibling `isBase64Encoded` → `LOG_REDACTED_VALUE` (base64 is reversible);
  - a sibling `headers` dict (key `headers`, any case) has a `content-type` (any case) whose
    whitespace-trimmed value starts with `multipart/`, or the body itself has the multipart shape
    (starts with `--`, contains `content-disposition:` in any case) → `LOG_REDACTED_VALUE`
    (multipart payloads are opaque to the redactor);
  - stripped body starts with `{` or `[` → `json.loads` + `_redact_for_logging` +
    `json.dumps(..., ensure_ascii=False)`, all inside one `try` catching `(ValueError,
    RecursionError)`: either failure — too deeply nested, or not valid JSON — returns
    `LOG_REDACTED_VALUE` (a truncated payload may still carry secrets, and `parse_qsl` would
    leave them inside a parameter *name*, so a JSON-looking body is never redacted as a form
    instead);
  - form-encoded — a sibling `headers` dict has a `content-type` (whitespace-trimmed) starting
    with `application/x-www-form-urlencoded`, or the body matches the
    `^[^=&\s]+=[^&\s]*(&[^=&\s]+=[^&\s]*)*$` shape → `_redact_query_string(body)`;
  - anything else → unchanged.

## Trade-offs

- Substring matching over-redacts look-alike keys (`cookie_consent`, `signature_version`);
  accepted for a log-only helper.
- Tokens nested inside the values of non-sensitive query parameters or JSON fields (e.g. a
  redirect URL carrying an `access_token`) are not masked.
- Values joined with `;` stay part of the previous parameter's value — Python's `parse_qsl` and
  API Gateway do not split on `;` — so a token appended after a `;` is masked only when the
  parameter it follows is itself sensitive.
- JSON strings inside keys other than `body` — e.g. an SNS `Records[].Sns.Message` payload — are
  not parsed.
- Header names outside `LOG_SENSITIVE_KEY_PARTS` (e.g. `X-Auth-Key`, `X-Session-Id`, `sessionid`)
  are kept; the price of extending the list is over-redaction of look-alike names.
- A body that starts with `--` and contains `content-disposition:` is masked as multipart by
  shape alone, without any content-type header — coarse on purpose, so a look-alike text body is
  over-masked.
- Logging is best-effort: secrets must not be sent in fields that are bound for the logs by
  design.
- The handler log lines keep the pre-existing `logger.info(<object>)` shape without a format
  string on purpose: the powertools Logger emits a dict message as a structured JSON object that
  CloudWatch queries depend on, and wrapping it in `"%s"` would change the log format for every
  consumer.
- The redacted copy is built even when the INFO level is disabled for the handler logger, and
  the pre-existing `logger.info(self.config)` and Processor result log lines stay as they are;
  an `isEnabledFor` guard and those lines are left to a follow-up ticket.
- The re-serialized JSON body is a log copy only — key order is preserved but whitespace differs
  from the original.

## Testing

Unit tests cover proxy events (v1 and v2), mixed-case and non-string keys, containers and
scalars, the `None`/bool/counter-word rules, the raw query string and every body path (JSON
object, JSON array, non-ASCII JSON, malformed JSON — including form-shaped bodies under every
content-type declaration —, multipart with headers, by shape and whitespace-padded content types,
plain text, form-encoded with and without the content-type header, base64 flag, 2000-deep JSON
nesting), the redaction guards at helper and handler level (recursion and the lone-surrogate
`UnicodeEncodeError`), plus handler-level assertions that no secret value reaches any
`logger.info` call while the Processor and the caller keep the original event and result objects.
