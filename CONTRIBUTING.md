# Contributing

Thanks for contributing to Radar Platform.

## Before You Open A PR

- Open or confirm an issue for bugs, regressions, or larger feature work.
- Keep changes focused. Separate refactors from behavior changes when possible.
- Update docs when setup, configuration, or runtime behavior changes.
- Add or update tests when behavior changes.

## Local Checks

Run the relevant checks before opening a pull request:

```bash
docker compose config
docker compose build
python3 -m pytest -q
npm ci --prefix frontend
npm run build --prefix frontend
```

If you use a local `.env`, keep it uncommitted. `.env.example` is the documented baseline for development defaults.

## Pull Request Expectations

- Describe the user-facing change and any operational impact.
- Call out new environment variables, ports, or migration steps.
- Include screenshots for meaningful frontend changes.
- Keep CI green before requesting review.

## Commit And Style Notes

- Follow the existing project structure and naming.
- Prefer small, readable patches over broad rewrites.
- Do not commit generated frontend build artifacts such as `*.tsbuildinfo` or emitted `vite.config.js` files.

## Security

Do not open public issues for security vulnerabilities. Follow [SECURITY.md](SECURITY.md) instead.
