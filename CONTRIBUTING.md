### Changing a Data Model

### Changing only Views
The data models are kept in `cognite/powerops/custom_modules`. Each set of data models are kept in a
module following the structure of the [CDF-Toolkit](https://developer.cognite.com/sdks/toolkit/). In each
module, you will find the data model files in the `data_models` resource folder. Each container, view, data model,
and space are kept in separate files, one file for one resource.

You can control which module is deployed by setting the `selected_module_and_packages` in
`cognite/powerops/config.dev.yaml`:

```yaml
...
  selected_modules_and_packages:
  - power_model_v1
...
```
This will deploy the `power_model_v1` module.

To change a data model, follow these steps:

1. Do the change to the `.yaml` files in the `data_models` folder.
2. Run the command `powerops init --dev --dry-run` to see what change will be made.
   -  Note the `--dev` flag is used to indicate the data model is currently in development mode.
      That means that when there are breaking changes to `views` and `data_models` resources, these will
      be deleted and recreated. In a production mode, you will have to bump the version of these resources
      instead.
   - You can add the flag `--verbose` to get more information about the changes that will be made.
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


### Changing Containers
Changing the containers requires dropping existing containers and recereating the changed ones. This is currently
not supported by the `powerops` CLI, so you will have to do it using `cdf-tk`, i.e., `cognite-toolkit`, directly.

**Note** There is currently no support for only dropping a single container, you will have to drop every container
in the data model(s) and recreate them.

To authenticate with the `cognite-toolkit` you will have to use a `.env` file which should look like this:
```dotenv
CDF_CLUSTER=bluefield
CDF_URL=https://bluefield.cognitedata.com
CDF_PROJECT=power-ops-staging
IDP_TENANT_ID=431fcc8b-74b8-4171-b7c9-e6fab253913b
IDP_CLIENT_ID=***
IDP_CLIENT_SECRET=***
IDP_TOKEN_URL="https://login.microsoftonline.com/431fcc8b-74b8-4171-b7c9-e6fab253913b/oauth2/v2.0/token"
SENTRY_ENABLED=false
```

When using the `cognite-toolkit` you have to build the configurations first and then deploy/clean them.

1. Build the configurations:
   ```bash
   cognite-tk build cognite/powerops --env dev
   ```
2. Dry-run the redeploy:
   ```bash
    cognite-tk deploy --drop-data --drop --env=dev --dry-run
    ```
3. Deploy the changes:
   ```bash
    cognite-tk deploy --drop-data --drop --env=dev
    ```
4. Verify in the UI that the changes are as expected.
