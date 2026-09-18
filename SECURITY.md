# Reporting a vulnerability

Email **security@acemq.com** with what you found and how to reproduce it. Please do not
open a public issue for anything exploitable.

You should get an acknowledgement within two working days, and an assessment of whether
it is a vulnerability, what is affected, and a rough timeline within a week. If a fix is
warranted, we will tell you when it is released and credit you unless you would rather
we did not.

## What is in scope

The `acemq-amqp-fastapi` package: the settings binding, the lifespan, the consumer
registry, the request dependencies and the health check.

Things worth reporting even if they feel minor:

- A settings binding that weakens the connection's security compared with what was
  configured — TLS not required when `acemq.tls` asked for it, verification quietly
  reduced, development certificates accepted without the explicit opt-in.
- A broker password, key passphrase or encryption key appearing in a log line, an
  exception message, a health response, or an OpenAPI schema. `acemq.password` and
  `acemq.tls.client_key_password` are `SecretStr` and `AceMQSettings.broker_url` is
  built without credentials on purpose; a path that defeats either is a finding.
- A health response exposing more than liveness, readiness and counters — queue
  contents, credentials, connection strings or message bodies.
- A dependency handing one application's connection to another application in the same
  process, or handing out a connection from a previous lifespan.
- A consumer registration that causes a message to be acknowledged before its handler
  has returned successfully, since that turns a crash into a silently dropped message.
- A drain that reports `finished` when messages were in fact abandoned.

## What is not

- **Your health route being reachable.** Whether `/health/acemq` is public is your
  decision; mounting it is an explicit line in your application. Authorising it is
  FastAPI's dependency system's job.
- **A password in a `.env` file.** Where configuration lives is yours to decide.
- **Vulnerabilities in FastAPI, Starlette, pydantic, aio-pika or aiormq** — report those
  to their projects.
- **Vulnerabilities in `acemq-amqp`** — report those against
  [that repository](https://github.com/AceMQ-Company/acemq-python-amqp), or here if you
  are unsure which it is.
- **`blocked: null` in a health response.** It is documented: there is no supported way
  to read aio-pika's blocked state, and `null` is the honest answer when the private
  accessor is not found.
- Findings from a scanner with no demonstrated impact.

## Supported versions

Pre-1.0, only the latest release. There are no maintenance branches yet, so a fix means
a new patch version.

## What this package does not do for you

It wires the library into a FastAPI application. It does not secure your routes, decide
where your secrets live, authorise who may publish what, or make a message safe to
handle twice.
