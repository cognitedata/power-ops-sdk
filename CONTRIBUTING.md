# Changing Data Modeling Resources

Data Modeling resources are deployed, updated, and managed via the [CDF-Toolkit](https://developer.cognite.com/sdks/toolkit/).
Refer to the documentation for detailed configuration options.

The relevant data modeling configuration files can be found in `toolkit/custom_modules` following the folder
and file naming structure required by CDF-Toolkit. Each space, container, view, and data model is kept in separate
files with one file for one resource.

You can control which module is deployed by setting the `selected_module_and_packages` in
`toolkit/config.dev.yaml`:

```yaml
...
   selected_modules_and_packages:
   - power_model_v1
...
```

This will only deploy the `power_model_v1` module.

## Authentication

To authenticate with the `cognite-toolkit` you will have to use a `.env` file in the `power-ops-sdk` root directory,
which should look like this:

```dotenv
CDF_CLUSTER=<CLUSTER>
CDF_URL=https://<CLUSTER>.cognitedata.com
CDF_PROJECT=<PROJECT>
IDP_TENANT_ID=431fcc8b-74b8-4171-b7c9-e6fab253913b
IDP_CLIENT_ID=***
IDP_CLIENT_SECRET=***
IDP_TOKEN_URL="https://login.microsoftonline.com/431fcc8b-74b8-4171-b7c9-e6fab253913b/oauth2/v2.0/token"
SENTRY_ENABLED=false
```

## Change Limitations

Not all resource types can be changed, and not all operations are supported to be made on existing resources. Refer to
the [Data Modeling changes documentation](https://docs.cognite.com/cdf/dm/dm_concepts/dm_containers_views_datamodels/#impact-of-changes-to-views-and-data-models)
for a detailed list of what is allowed. Based on if an operation is allowed or not there are a few different ways to
deploy the changes.

### Changes to Containers

If the change is allowed and it *is not* a breaking change then no version or additional considerations need to be taken
into account.

If the change is allowed and it *is* a breaking change then the change needs to be carefully planned to scope it's impacts.

If the change is not allowed (like deleting a property) then alternative workarounds need to be implemented (ie. do not
use the property we want to delete).

### Changes to Views and Data Models

Many changes to containers may result in changes to the related views and data models, these should then potentially be
versioned in order to avoid breaking changes in the consumption layer.

If a change requires a version change, the following version system should be used (MAJOR.MINOR.PATCH):

- Test changes to data model in `power-ops-dev` with a version bump to the PATCH
- Test changes to data model along with related resources (functions, workflows, shop) in `power-ops-staging` with a version bump to MINOR
- Changes deployed to customer environment (dev & prod) should bump the MAJOR version
- Once deployed to the next environment the previous environment will "rest"
  - If testing is done in `power-ops-dev` with version 1.0.3, then those models will be deployed to `power-ops-staging`
   as 1.1.0. When testing is done in staging and changes get deployed to customer environments the version would become 2

## Local development process

1. Make the relevant changes to the `.yaml` files in the `data_models` folder.
2. Post on slack `#powerops-backend` that you are about to make changes to ensure it doesn't conflict with others work.
3. Deploy changes manually to **power-ops-dev** using toolkit following bellow steps.
4. Verify in the UI that the changes are as expected.
5. Iterate until desired changes are completed.
6. Create a PR and request feedback on your suggested changes.
7. Once PR is approved inform the team that the changes will be deployed to `power-ops-staging`.
8. Deploy changes manually to **power-ops-staging** ensuring you've used the correct version if needed.
9. Regenerate the SDK for the data model changes by calling `python scripts/pygen_generate_clients.py`.
10. Bump the SDK version in `pyproject.toml` and `cognite/powerops/_version.py`.
11. Update the `CHANGELOG.md` with the changes made.
12. Get a second approval on the PR.

## Manual Deployment

1. Check which environment to deploy to, they are defined in the `cognite` folder in the `config.<ENV>.yaml` files
   1. **config.dev.yaml**
      1. CDF_PROJECT: `power-ops-dev`
      2. ENV: `dev`

         ```bash
         export ENV="dev"
         ```

   2. **config.staging.yaml**
      1. CDF_PROJECT: `power-ops-staging`
      2. ENV: `staging`

         ```bash
         export ENV="staging"
         ```

   3. **config.lyse-dev.yaml**
      1. CDF_PROJECT: `lyse-dev`
      2. ENV: `lyse-dev`

         ```bash
         export ENV="lyse-dev"
         ```

   4. **config.lyse-prod.yaml**
      1. CDF_PROJECT: `lyse-prod`
      2. ENV: `lyse-prod`

         ```bash
         export ENV="lyse-prod"
         ```

   5. **config.heco-dev.yaml**
      1. CDF_PROJECT: `heco-dev`
      2. ENV: `heco-dev`

         ```bash
         export ENV="heco-dev"
         ```

   6. **config.heco-prod.yaml**
      1. CDF_PROJECT: `heco-prod`
      2. ENV: `heco-prod`

         ```bash
         export ENV="heco-prod"
         ```

2. Ensure the credentials in your `.env` file point to the same environment you want to deploy to
3. Build the configurations and remove existing files in the build directory:

   ```bash
   cdf-tk build toolkit/ --env=$ENV
   ```

4. Dry-run the deployment to see what changes would be made:

   ```bash
   cdf-tk deploy --env=$ENV --dry-run
   ```

5. Deploy the changes if the environment and changes your making won't impact other people's work:

   ```bash
   cdf-tk deploy --env=$ENV
   ```

### WARNING: Full manual redeploy steps

We should NOT be needing to delete containers to make changes to them but we are limited to only making changes based on
the limitations detailed [here](https://docs.cognite.com/cdf/dm/dm_concepts/dm_containers_views_datamodels/#impact-of-changes-to-views-and-data-models).
In the scenario that any container changes need to be made that are not allowed via update the container will need to be
deleted and recreated again, this should only be done on a case by case bases taking into consideration if the data will
need to be re-ingested or not.

DO NOT perform these steps unless it has been aligned with the full team and a solution has been agreed upon.

1. Follow the same 1-3 steps from the [Manual Deployment](#manual-deployment) steps
2. Dry-run the deployment to see what changes would be made with the `drop` flags:

   ```bash
   cdf-tk deploy --drop-data --drop --env=$ENV --dry-run
   ```

3. Deploy the changes if the environment and changes your making won't impact other people's work:

   ```bash
   cdf-tk deploy --drop-data --drop --env=$ENV
   ```
