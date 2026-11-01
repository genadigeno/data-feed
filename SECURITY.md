# Security policy

## Reporting a problem

Please don't report security problems in a public issue. Use
[GitHub's private reporting](https://github.com/genadigeno/data-feed/security/advisories/new) instead, and include what you found, how to reproduce it, and
what an attacker could do with it.

This is a project maintained in spare time, so replies can't be promised by a set date, but reports are taken seriously and credited if you wish.

## Supported versions

Fixes go into the latest release. Please upgrade before reporting.

## Running datafeed safely

- Without authentication, datafeed only listens on loopback. Off loopback, set `application.environment: production` and `auth.mode: token`, and use TLS. The
  README's "Going to production" section lists what's required.
- `docker-compose.yml` has no authentication and is for local use only.
- Tokens are shown once, and only their hashes are stored. Keep them out of command lines and logs: use `file:PATH` or `${NAME}` references.
- Generated data is synthetic, but a catalog holds whatever you put in it, so don't put real personal data there.
