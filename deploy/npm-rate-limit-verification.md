# NPM Login Rate-Limit Verification

Complete this deployment record against the active production proxy before
exposing the login routes publicly. This operational verification is separate
from the locally verified UM-163 implementation. Do not include credentials,
tokens, or unredacted generated configuration.

## Active Configuration

- Verification date:
- Operator:
- Proxy host:
- NPM version:
- Trusted client-IP source and trust boundary:
- Login request rate:
- Login burst:
- Edge rejection status and response behavior:
- Generated configuration backup location:
- `nginx -t` result:
- Reload result:

## Required Probes

Record the observed status, `Retry-After` behavior where present, and relevant
redacted access-log entry for each probe.

| Probe | Expected result | Observed result |
| --- | --- | --- |
| Repeated `POST /login` from one client IP | Edge limit reached | |
| Repeated `POST /api/auth/login` from one client IP | Edge limit reached | |
| `GET /login` while POST limit is reached | Remains available | |
| `POST /login?probe=1` | Same limit as `/login` | |
| `POST /api/auth/login?probe=1` | Same limit as API login | |
| Trailing-slash variants | Cannot bypass the configured policy | |
| Requests with spoofed `X-Forwarded-For` values | Same real-client bucket | |
| Requests from a second client IP | Independent bucket | |
| Valid login after refill/cooldown | Succeeds | |
| Signed upload/download transfer | Not throttled by login policy | |
| Internal worker request bypassing NPM | Remains usable | |

## Rollout Approval

- [ ] Both public login POST paths are limited by trusted client IP.
- [ ] Caller-supplied forwarding headers cannot select a rate-limit bucket.
- [ ] Ordinary login page views remain available.
- [ ] Signed transfers and internal worker traffic are unaffected.
- [ ] Generated Nginx configuration validates and NPM reload succeeds.
- [ ] Actual rates, bursts, status, and response behavior are documented above.
