# Releasing

A release is a tag. `release.yml` picks it up, checks everything it can, builds
the sdist and the wheel, and adds them to the AceMQ package index — a directory
of files in `AceMQ-Company/pypi`, served at <https://acemq.org/pypi/> and walked
by pip under PEP 503. There is no PyPI upload here, no Trusted Publishing and no
API token: publishing is a commit pushed with a deploy key.

That is worth knowing before the first tag, because it is the property that makes
the guards below tolerable rather than terrifying. A file on this index can be
deleted and the index regenerated. A version on PyPI cannot: its filename is
spent forever, and yanking it hides it from resolution while leaving every
lockfile that already names it working.

## Before tagging

1. **Set the version in both places that declare it.** They are checked against
   the tag and a release stops if either disagrees:

   | | |
   |---|---|
   | `pyproject.toml` | `version = "0.1.0"` — what the wheel's metadata carries |
   | `src/acemq_fastapi/__init__.py` | `__version__ = "0.1.0"` — what a running process reports |

   The second one is not decoration. An operator reading `acemq_fastapi.__version__`
   off a live service is reading the only version string that survives the
   install, and a package whose metadata and `__version__` disagree tells two
   different stories about the same deployment.

2. **Move `## [Unreleased]` in `CHANGELOG.md` down to the version being
   released**, dated, and leave `## [Unreleased]` in place and empty. Add the
   two link definitions at the foot of the file.

3. **Check the `acemq-amqp` constraint still says what you mean.** It is in
   `pyproject.toml` and it is part of the release:

   ```
   acemq-amqp[rabbitmq]>=0.7.0,<0.8
   ```

   `release.yml` reads that pin back out of the **built wheel** and refuses a
   release whose metadata lost it. The floor is a real requirement — the health
   check calls `Connection.blocked`, which 0.7.0 added — and the ceiling is there
   because a pre-1.0 library puts its breaking changes in minor bumps.

4. **Commit both, push to `main`, and let CI go green.**

5. **Tag, annotated, and push.**

   ```bash
   git tag -a v0.1.0 -m "AceMQ for FastAPI 0.1.0"
   git push origin v0.1.0
   ```

   Annotated rather than lightweight: a release tag carries a date and an author,
   and `git describe` reads one and ignores the other.

## What the workflow checks

In order, and it stops at the first thing that is wrong. Everything up to
**Commit and push** leaves the index untouched, so a failure before that point
has published nothing.

- **The version is a version.** An allow-list, not an escape: the string reaches
  a filename, a commit message and a package version, so anything that is not
  plainly a version number is refused.
- **The version is on this package's line.** See below.
- **`pyproject.toml` and `__version__` both say what the tag says.**
- **The suite passes** on Python 3.10 — the floor `requires-python` promises —
  with `ruff check .` and a strict `mypy` over `src` and `tests`.
- **The sdist and the wheel both build**, and `twine check --strict` passes on
  both.
- **The built wheel carries the intended version and the pin.** Read out of the
  wheel's own `METADATA`, not out of the tree it was built from: the artifact is
  what people install, so the artifact is what is checked.
- **The sdist really builds the package.** Installed alone into an interpreter
  that has never seen this checkout, with its dependencies resolved from the
  index, then imported and constructed. An sdist missing a file installs cleanly
  and fails on import, and hatchling's file selection is configuration rather
  than a guarantee. This is also the first proof that the `acemq-amqp` this
  release pins is actually resolvable from where consumers are told to look.
- **The integration suite passes against a real broker**, with
  `ACEMQ_TEST_BROKER_CTL` set so the blocked-connection test runs rather than
  skips, and a count check afterwards — a suite that skipped everything reports
  the same green as one that passed everything.
- **`PYPI_REPO_DEPLOY_KEY` exists**, checked before the index is touched.
- **The published distribution installs from the index.** A fresh virtualenv,
  `--index-url https://acemq.org/pypi/simple/` with PyPI only as the extra, with
  dependencies rather than `--no-deps`, then `__version__` compared and `AceMQ()`
  constructed. GitHub Pages takes a moment to serve a new commit, so this retries
  for five minutes rather than racing the deploy.

## The credential

**`PYPI_REPO_DEPLOY_KEY`** — a repository secret on
`AceMQ-Company/acemq-python-amqp-fastapi`, holding the **private half of an SSH
deploy key with write access to `AceMQ-Company/pypi`**, and to nothing else in
the organisation. The public half goes on that repository as a deploy key with
*Allow write access* ticked. The same key the Python library's release uses; a
key of its own is just as good.

A deploy key rather than a personal access token because a token carries its
owner's reach across every repository they can see, and this job needs exactly
one.

**Without it a release fails, loudly, before anything is published.** An absent
secret interpolates to an empty string and `actions/checkout` quietly falls back
to the default token when handed one, so the workflow checks for the secret
itself rather than letting that happen and discovering it at the push. The error
names the secret.

## The version line

**`0.1.x`, until somebody decides otherwise.** `release.yml` refuses any other
version, from a tag or from a dispatch, with an error saying so. A mistyped
`v1.0.0` fails a run instead of claiming a version number this package has not
earned.

Lifting it is one edit, in the `case` in the *Work out the version* step of
`release.yml`:

```sh
case "$VERSION" in
  0.1.*) ;;
```

Change it in the same commit that bumps `pyproject.toml` and `__version__`, so
moving the line is a decision with a diff rather than a typo that got through.

## Why this versions apart from the library

`acemq-amqp` is at 0.7.x. This package starts at 0.1.0, and the two numbers are
not going to converge — the same arrangement the Spring Boot starter has with
`acemq-java-amqp`, for the same reason.

This package tracks **FastAPI's and Starlette's release trains as much as
AceMQ's**. A FastAPI release that moves the lifespan contract is a release this
package has to answer, and waiting for the library to cut a version to do it
would put the two on a schedule neither of them chose. The reverse holds too: a
library release that changes nothing above the transport should not oblige this
package to reissue itself.

What ties them is the pin, not the number. `>=0.7.0,<0.8` is the statement about
which library this release works with, it is in the wheel's metadata where a
resolver can act on it, and `release.yml` checks it is still there.

## Re-running a release

`workflow_dispatch` takes a version and runs the whole thing. It is for the case
where a release failed **after** its checks — a missing secret, an index that was
not serving yet — and there is nothing wrong with the commit.

Use it rather than deleting and re-pushing the tag. A tag is the name of a
release, and moving one makes two different commits answer to the same name, in
a repository where somebody may already have fetched the first.

A dispatch on a branch runs every check and **stops short of publishing** — the
publish job is gated on `refs/tags/v*`. That is what makes the workflow safe to
try out.

Publishing twice is safe in itself. `publish.sh` on the index keeps the bytes a
version was published with and says so, because a wheel is not byte-reproducible
— the archive carries timestamps — and two machines holding different code under
one version is the failure worth preventing. Changing the code needs a new
version number.

## After a release

Confirm the index is actually serving it, the way a consumer would:

```bash
pip download --no-deps --dest /tmp/check \
  --index-url https://acemq.org/pypi/simple/ \
  --extra-index-url https://pypi.org/simple/ \
  "acemq-amqp-fastapi==0.1.0"
```

and that the project page lists both files:

```bash
curl -s https://acemq.org/pypi/simple/acemq-amqp-fastapi/
```

Two distributions per release — `acemq_amqp_fastapi-<version>-py3-none-any.whl`
and `acemq_amqp_fastapi-<version>.tar.gz`. One of them missing means the publish
step ran on a partial artifact, which is worth chasing even though pip will not
notice.
