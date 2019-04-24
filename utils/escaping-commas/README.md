# Escaping commas in parameters

## Example

When invoking a template we pass the `parameters` as a dictionary of comma-separated key-value pairs ([docs](https://cloud.google.com/dataflow/docs/guides/templates/executing-templates)). But sometimes we might need to actually use a comma symbol within a parameter and escaping it with (double) quotes does not work. We can resort to a different separator as explained in gcloud [docs](https://cloud.google.com/sdk/gcloud/reference/topic/escaping).

For example, we can switch to the `:` symbol:

```bash
gcloud dataflow jobs run comma_delimiter_gcloud \
  --gcs-location gs://$BUCKET/templates/$TEMPLATE \
  --parameters ^:^delimiter=,:userAgent=test
```

which results in the following job parameters (as expected):

![parameters](https://user-images.githubusercontent.com/29493411/56682168-4c89bc00-66cb-11e9-8f05-6beedb37e327.png)

Of course, the `:` symbol is frequently used in parameters such as URLs (gs:// paths and so) but a non-conflicting one (or even multiple separator characters) can be specified between the `^` symbols.

## License

These examples are provided under the Apache License 2.0.

## Issues

Report any issue to the GitHub issue tracker.
