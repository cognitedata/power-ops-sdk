# Setup

This page summarizes how to set up a new PowerOps project.

## Initial CDF Setup

1. Create and configure a CDF project.
2. Set up required access groups and datasets.
3. Prefer toolkit for project setup tasks.

The `toolkit/modules/power_ops_template/` module contains example access groups and baseline PowerOps datasets.

After initial setup, continue with project-admin credentials for the next steps.

## PowerOps Toolkit

- Provide credentials with enough privileges to run toolkit.
- Store these credentials as GitHub Environment secrets for CI/CD usage.
- Ensure the GitHub Environment name matches the CDF project name exactly.
- Add the new project in the toolkit release workflow where required.
- Create a PR with toolkit config for the new project (see the staging config as a reference).

Important:

- The CI/CD in this repository deploys the PowerOps data model resources.
- Customer-specific resources should be managed in a separate customer repository.

## CogShop and PowerOps UI

Provide the PowerOps team credentials and project details:

- CDF cluster
- CDF project name
- Tenant ID
- Client ID
- Client Secret

These credentials are used for CogShop configuration and validation flows.

## Functions and Workflows

If PowerOps functions are used:

- Provide credentials for the PowerOps functions repository so deployments can be automated.
- Use workflow examples in the PowerOps template module as a baseline for function-related workflows.
