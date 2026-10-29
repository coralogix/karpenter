# Duration annotations share validation

Status: accepted

Reclamation delay and evacuation timeout use one duration parser: absent values use their defaults (15 seconds and 5 minutes respectively); empty, malformed, or negative values use the same defaults and raise an event on the NodePool. Zero is accepted literally: `0s` adds no reclamation wait and makes an evacuation time out immediately, without an invalid-value event.

We accept these consequences of bad operator configuration to keep validation simple. Special handling to make zero evacuation timeouts useful or to preserve negative reclamation delays as no-wait values would add complexity without enough value. Operators must supply a suitable duration; the controller does not correct a syntactically valid zero into a useful timeout.

This supersedes the negative-delay semantics in ADR-0002 and ADR-0003 and the earlier issue 05 requirement to reject zero evacuation timeout. The specs, issues, and operator documentation follow this decision. No compatibility handling or code change is required.
