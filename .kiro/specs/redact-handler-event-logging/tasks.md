# Handler log redaction — Tasks

- [x] 1. Redaction helper: sensitive-key matching, `None`/bool kept, counter-like numerics kept
  (`LOG_COUNTER_KEY_PARTS`), recursive, non-mutating.
- [x] 2. `rawQueryString`: mask values of sensitive query parameters, keep the rest readable.
- [x] 3. String `body`: redact parsable JSON recursively, skip base64, keep other bodies as is.
- [x] 4. Handler logs redacted copies of event and result; original objects pass through.
- [x] 5. Unit tests to 100% coverage, including handler-level no-leak assertions.
- [x] 6. Docs: `#:`-documented constants, docstrings, agent-guide bullet, this spec entry.
- [x] 7. Whole-word counter matching (`LOG_COUNTER_KEY_WORDS`, split on non-alphanumerics and
  camelCase); form-encoded and base64-flagged bodies redacted (round-2 review).
- [x] 8. Recursion guards: a 2000-deep JSON body or event logs `LOG_REDACTED_VALUE`; the handler
  never fails because of logging (round-2 review).
- [x] 9. Docs section "Logging of the event and the result" in `docs/concepts/processor.rst`;
  remaining gaps listed as trade-offs here and in the docs.
