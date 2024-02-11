### Changing a Data Model

The data models are kept in `cognite/powerops/custom_modules`. Each set of data models are kept in a
module following the structure of the [CDF-Toolkit](https://developer.cognite.com/sdks/toolkit/). In each
module, you will find the data model files in the `data_models` resource folder. Each container, view, data model,
and space are kept in separate files, one file for one resource.

To change a data model, follow these steps:

1. Do the change to the `.yaml` files in the `data_models` folder.
2. Run the command `powerops init --dev --dry-run` to see what change will be made.
   -  Note the `--dev` flag is used to indicate the data model is currently in development mode.
      That means that when there are breaking changes to `views` and `data_models` resources, these will
      be deleted and recreated. In a production mode, you will have to bump the version of these resources
      instead.
3. Create a PR and request feedback on your suggested changes.
4. Post in the development channel `#powerops-backend` that you are about to apply the changes.
3. Check that nobody else are currently about to do change, then, run `powerops init --dev` to apply the changes.
4. Verify in the UI that the changes are as expected.
5. Post in the development channel `#powerops-backend` that the changes are applied.
6. Regenerate the SDK for the data model changes by calling `python scripts/pygen_generate_clients.py`.
7. Commit the changes and push it to your PR.
8. Update the `CHANGELOG.md` with the changes you have made.
9. Update the version in `pyproject.toml` and `cognite/powerops/_version.py` to the next version. Do a patch version bump.
10. Get the PR approved and merge it.
