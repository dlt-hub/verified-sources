---
paths:
  - "sources/*/README.md"
  - "tests/*/README.md"
---

# Source README

`sources/<name>/README.md` is the maintenance document of a verified source, and `dlt init` copies it
into user projects. It answers two questions:

- **Changelog**: can I upgrade safely from the previous version? An entry per version, newest first:
  an **Upgrade** line (`safe`, or the steps and what happens without them, and whether the incremental
  state carries over), then short bullets starting with **breaking**, new, changed or fixed that name
  the resource whose data changes.
- **Maintenance & Troubleshooting**: what do I do when something goes wrong? Limits, quirks, failures
  and their workarounds learned from running against the upstream, each with its source: a run on the
  test account, a doc with its URL, or a PR.

The only other section is **Extension points**. Setup and usage belong in the dlt docs. No defaults,
page sizes or descriptions of how a resource works.

**Seeding** and **Tests** go to `tests/<name>/README.md`, not to the source README.

When writing or changing a README, follow
[Source version and README](../skills/refresh-source/SKILL.md#source-version-and-readme) of the
refresh-source skill.
