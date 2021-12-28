# Extras
This section contains the tools and recommendations for maintainers and contributors of `sosw`.

## Version Release
Make sure that you:
- Read and understand the PR from `X_X_X` branch to `master`.
- Update the version in `setup.py`.
- Update the Copyright year if needed.
- Make sure that all integration tests pass. Remember that status checks validate only unit tests.
- MERGE TO MASTER
- Check that the docs were built. Invalidate CDN if required.
- Check that the version is published in PyPI.
- Create a new branch for minor release. Update `setup.py` with new version.
- Create a PR to master from this branch.

## Docs build
- Make sure that the secrets for `.github/workflows` are up to date.
- Make sure that you have the docs hosted in some reliable location.
- Make sure that DNS points to that location.
- RTFM the guidelines of how to publish `sosw` docs and follow recommendations.

`docs_builder` folder contains the required CloudFormation templates and scripts to make this happen.
