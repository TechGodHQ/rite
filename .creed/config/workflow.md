# Git / PR Rules

- Use Shiv's global Git identity: `Shiv Rossi <shiv@fastmail.com>`. Do not add
  `Co-authored-by`, `Signed-off-by`, or other attribution trailers.
- Use conventional commits: `feat:`, `fix:`, `refactor:`, `docs:`, or `chore:`.
- Work on a focused branch and land changes through a pull request; never push
  directly to `main`.
- Every PR needs the required local gates and a review panel before handoff.
  Keep the PR description tied to its Linear issue and state deployment or
  live-validation boundaries precisely.
- Do not publish tags or releases without explicit authorization for that
  exact version. Tags are immutable.
