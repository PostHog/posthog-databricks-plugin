# PostHog Databricks Plugin

> ℹ️ This plugin is only available for self-hosted users. Users of PostHog cloud should look into [S3 batch exports](https://posthog.com/docs/cdp/batch-exports/s3) and [connecting databricks to S3](https://docs.databricks.com/en/storage/amazon-s3.html) as an alternative.
[![License: MIT](https://img.shields.io/badge/License-MIT-red.svg?style=flat-square)](https://opensource.org/licenses/MIT)

## Installation

### Get Following credentials

+ Domain name of the cluster provided by databricks
+ Generate an Api key by following this [documentation](https://docs.databricks.com/administration-guide/access-control/tokens.html)
+ Give a temporary filename path for saving the raw data.
+ Enter you cluster id used in the system [documentation](https://docs.databricks.com/workspace/workspace-details.html)
+ Give a database name where you want to store the data.
+ Enter events in comma ( , ) seprated way in order to ignore the data.

## This plugin will:

+ Push data from posthog to databricks every minute.
+ creates a table and runs scheduled job to perform migration of data from dbfs to database.

## Limitation

+ You can't sync the historic data.
+ You can't change frequency of data push to databricks.
+ You can't minimize frequency to less than 1 minute.

### [Join our Slack community.](https://join.slack.com/t/posthogusers/shared_invite/enQtOTY0MzU5NjAwMDY3LTc2MWQ0OTZlNjhkODk3ZDI3NDVjMDE1YjgxY2I4ZjI4MzJhZmVmNjJkN2NmMGJmMzc2N2U3Yjc3ZjI5NGFlZDQ)

We're here to help you with anything PostHog!
