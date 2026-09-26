# Security

This page is about reaching the broker safely: encrypting the connection, deciding who
the service logs in as, getting TLS working on a laptop without teaching it to trust
something it should not, and encrypting the message body so the broker never holds the
plaintext.

It is not about authenticating HTTP callers. That is FastAPI's, and nothing here
touches it.

Everything below is the library's `acemq_amqp.Security` underneath. `AceMQSettings` is a
builder for it: `build_security()` turns `acemq.username`, `acemq.password` and
`acemq.tls.*` into one `Security` and hands it to `acemq_amqp.connect`. Where a
capability has no setting, this page says so and shows the hand-wiring instead of
inventing configuration for it.

## Credentials

```bash
export ACEMQ_URL=amqps://broker.internal:5671/
export ACEMQ_USERNAME=orders-service
export ACEMQ_PASSWORD=...
```

`acemq.password` is a pydantic `SecretStr`, so it prints as `**********` in a `repr` and
in the traceback of anything that happens to be holding the settings object.

The reason the login is separate from the URL is worth stating, because putting it in
the URL also works and is what most examples do. `AceMQSettings.broker_url` — the
property the lifespan dials and the property every error message prints — applies
`acemq.virtual_host` and nothing else. The credentials travel in the `Security` object
instead, which is where the library puts them. So a connection string in a log, a
timeout message naming the broker, or a Sentry breadcrumb carries no password:

```
acemq-fastapi: the broker at amqps://broker.internal:5671/ did not answer within 10s
```

With the login in the URL that line carries the password. There is no setting that
strips it back out.

### A credential read from a file, or re-read on reconnect

The library has more than one way to produce credentials:
`acemq_amqp.credentials_from_environment(...)` and
`acemq_amqp.credentials_from_file(path)` return a `CredentialsSource` — a callable the
library asks each time it needs a login, which is how a rotated secret reaches a
reconnect without a restart.

**Neither is reachable through a setting.** `AceMQSettings.build_security()` builds a
fixed `Credentials` from `acemq.username` and `acemq.password`, read once at start-up.
For a mounted secret that rotates underneath a running process, build the connection
yourself:

```python
import acemq_amqp
from acemq_amqp import Connection, Security, credentials_from_file
from acemq_fastapi import AceMQ, AceMQSettings


async def open_connection(settings: AceMQSettings) -> Connection:
    return await acemq_amqp.connect(
        settings.broker_url,
        codec=settings.build_codec(),
        origin=settings.client_name,
        retry=settings.build_retry(),
        prefetch=settings.prefetch,
        max_outstanding_publishes=settings.max_outstanding_publishes,
        confirm_timeout=settings.confirm_timeout,
        security=Security(
            certificate_authority=settings.tls.certificate_authority,
            credentials=credentials_from_file("/run/secrets/broker"),
        ),
    )


acemq = AceMQ(connection_factory=open_connection)
```

`connection_factory` is the seam for every case this package does not model, and this is
one of them. It takes the settings and returns an open `acemq_amqp.Connection`; the
lifespan, the registry, the dependencies, the health check and the drain all work the
same way against it. Copy the argument list from
[`AceMQ._open`](apidocs/acemq_fastapi/integration.html) and change the part you need —
leaving one out silently drops that setting, which is the one hazard of this seam.

## TLS

TLS is asked for by the scheme. `amqps://` is encrypted, `amqp://` is not, and
`acemq.tls.*` is only consulted for the former — the library **refuses** TLS settings
against a plaintext URL rather than ignoring them, and this package passes that refusal
straight through as a `SecurityError` out of the lifespan. A URL typed `amqp://` with a
certificate authority configured fails to start instead of quietly connecting in the
clear.

The whole table is in [configuration](configuration.md#tls--acemqtls); what follows is
what the settings mean.

```bash
export ACEMQ_URL=amqps://broker.internal:5671/
export ACEMQ_TLS__CERTIFICATE_AUTHORITY=/etc/ssl/acemq/ca.pem
```

**Naming an authority replaces the machine's trust store rather than adding to it.**
That is deliberate and it is the setting people are most often surprised by. A broker
holding a certificate from a public authority is not your broker: if `ca.pem` were one
more trusted root alongside the system bundle, anything with a certificate from any
public authority would satisfy the check. One authority, and only that one.

`server_name` is for the case where the name on the certificate is not the host in the
URL — a broker reached through a service mesh, or by an address. Setting it checks the
certificate against that name instead. It does not switch the check off.

### Mutual TLS

```bash
export ACEMQ_TLS__CLIENT_CERTIFICATE=/etc/ssl/acemq/orders-service.pem
export ACEMQ_TLS__CLIENT_KEY=/etc/ssl/acemq/orders-service.key
export ACEMQ_TLS__CLIENT_KEY_PASSWORD=...
```

`client_key` defaults to the certificate file, for the common case where both are in one
PEM. `client_key_password` is a `SecretStr` like `acemq.password`.

A broker configured for `EXTERNAL` authentication takes the identity from the
certificate and wants no username at all. Leave `acemq.username` and `acemq.password`
unset: `build_security()` produces a `Security` with `credentials=None` and the
certificate is the login.

### Turning verification off

```bash
export ACEMQ_TLS__VERIFICATION=nothing-at-all
```

Encrypted, and to whoever answered. It is spelled `nothing-at-all` rather than
`verify: false` because a configuration file should not be able to hide what it is
giving up behind a word that reads like a formality. There is one situation this is for
— a broker on a host you control, reached over a network you control, while you work out
why the real certificate is not being accepted — and it should not survive the
afternoon.

The library's own spelling of the same thing is
`acemq_amqp.without_verifying_the_broker()`, whose docstring says what it costs.

## Development certificates

Generating a self-signed certificate for local TLS is the point at which a project
teaches a machine, and then a base image, and then a deployment, to trust something it
should not. The library answers that by generating certificates that carry a marker and
then refusing them:

```bash
pip install "acemq-amqp[crypto]"
python -m acemq_amqp.devcerts --directory certs --broker localhost --days 30
```

That writes a certificate authority, a broker certificate and key, a client certificate
and key, and a `rabbitmq.conf` pointing the broker at them. Every certificate carries
the string `ACEMQ DEVELOPMENT ONLY - DO NOT TRUST`, and every AceMQ library refuses one
however trust is configured — it is not a convention, it is a check in the handshake.

Which means a development certificate needs one more setting, and only in development:

```bash
export ACEMQ_URL=amqps://guest:guest@localhost:5671/
export ACEMQ_TLS__CERTIFICATE_AUTHORITY=certs/ca.pem
export ACEMQ_TLS__ALLOW_DEVELOPMENT_CERTIFICATES=true
```

`allow_development_certificates` is off by default, so a `.env` copied from a laptop into
a deployment fails at start-up with a `SecurityError` naming the marker, rather than
working. That is the whole design: the failure happens on the machine where the mistake
is cheap to fix.

`acemq_amqp.is_development_certificate(pem_bytes)` answers the same question from your
own code, which is what a start-up assertion in a staging environment can use.

The `devcerts` module is a library API as well as a command — `acemq_amqp.devcerts.generate(...)`
returns a `GeneratedCertificates` naming every file it wrote — so a test fixture can
generate a set into a temporary directory and throw them away.

## Encrypting the payload

TLS protects the message in flight. It does nothing about the message sitting in a
durable queue on a broker whose disks somebody else backs up. `acemq_amqp.codecs.encrypted`
is the answer to that: an AES-GCM codec wrapping another codec, so the body on the wire
and on disk is ciphertext and the broker routes it without being able to read it.

```bash
pip install "acemq-amqp[crypto]"
```

**There is no `acemq.*` setting for this**, and there could not sensibly be one: a
setting that named a key file would put the key in the same configuration as the broker
URL. It is wired in code, and there are three places to do it depending on how much of
the application is encrypted.

### One format for everything

The cleanest of the three, because after it the rest of the application is unchanged.
`acemq.format` names a codec from the library's registry, and the registry takes
additions:

```python
# wiring.py — imported before the application is built
import os

from acemq_amqp import JsonCodec, register_codec
from acemq_amqp.codecs.encrypted import EncryptedCodec, EncryptionKey, Keyring, key_from_base64

keyring = Keyring(
    EncryptionKey("2026-01", key_from_base64(os.environ["PAYMENTS_KEY_2026_01"])),
)

register_codec("encrypted-json", lambda: EncryptedCodec(JsonCodec(), keyring))
```

```bash
export ACEMQ_FORMAT=encrypted-json
```

`register_codec` has to have run before the lifespan opens, because
`AceMQSettings.build_codec()` resolves the name inside `AceMQ.start()`. Importing the
module that calls it from the module that builds the `AceMQ` is enough. A name that is
not registered fails the start with a `KeyError` listing the names that are, which is
the right failure: the alternative is a service that starts and publishes plaintext.

### Per consumer and per publisher

When only some destinations are encrypted:

```python
payments = EncryptedCodec(JsonCodec(), keyring)


@acemq.consumer("payments", codec=payments)
async def handle(message: Message) -> Ack:
    ...


Payments = Annotated[Publisher, Depends(publishes("payments", codec=payments))]
```

Both `@acemq.consumer` and `publishes` take a `codec=`, and it overrides the
connection's for that consumer or that destination only.

### On the connection

`connection_factory`, as in the credentials section above, with
`codec=EncryptedCodec(JsonCodec(), keyring)` in place of `settings.build_codec()`. Worth
doing over `register_codec` only when the codec needs something that is not available at
import time.

### Rotation

A `Keyring` holds several keys and encrypts with one:

```python
keyring = Keyring(EncryptionKey("2026-01", january), EncryptionKey("2026-02", february))
keyring.use("2026-02")   # encrypt with February; still decrypt January
```

The key identifier is framed into the body, so a message written in January is read by a
process whose keyring still holds the January key, whatever it is encrypting with now.
Deploy the new key to every reader first, then switch the writers, then drop the old key
once nothing in any queue predates the switch. A reader handed a body whose key
identifier it does not hold refuses the message rather than guessing —
`acemq_amqp.codecs.encrypted.key_id_of(body)` is what tells you which key a stuck
message wants.

## The health route is not protected

`acemq.health_router()` returns an ordinary `APIRouter`. It carries no authentication,
and the report it answers with names the broker's state, the registered consumers and
their in-flight counts. On a service whose port is only reachable from the cluster that
is the point. On one that is not, put a dependency in front of it where it is mounted:

```python
app.include_router(acemq.health_router(), dependencies=[Depends(only_the_probe)])
```

`include_router(dependencies=...)` is FastAPI's, applies to every route in the router,
and is the mechanism rather than anything this package added. A readiness probe that has
to authenticate is also a readiness probe that fails when the authentication backend
does, so this is a decision rather than a default — which is why the router is mounted
explicitly in the first place.

## What is not here

**No authorisation model for messages.** A handler decides what a message is allowed to
do. The envelope's `origin` says which client published it and is stamped by the library,
not asserted by the publisher's payload, so it is worth reading — but it is a name, not
a credential.

**No secret management.** `pydantic-settings` reads the environment and a `.env` file.
Anything else — a mounted file, a vault agent, a sidecar — is the deployment's, and
reaches this package as an environment variable or through `connection_factory`.

**No TLS for the HTTP side.** That is uvicorn's, or the proxy's.

## Then

- [Configuration](configuration.md) — the `acemq.tls.*` table in full
- [Serialization](serialization.md) — codecs, and what `acemq.format` can name
- [Health](health.md) — what the route reports, and to whom
