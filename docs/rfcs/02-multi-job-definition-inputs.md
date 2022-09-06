# Multi-input support for job definitions

Author: @mlefebvre

Summary: Allow job definitions to support multiple inputs.

Status: accepted

## Background

At the moment, when a job needs to run periodically based on an inventory
and to also have the capability to be executed when a message is published to a topic
(e.g. when fetching pages from an API),
an almost identical job definition has to be created where the only difference is the input.

This is error-prone and makes the topologies harder to read and maintain.

## Proposal

We propose to modify the job definitions to also support multiple named inputs,
similarly to what is done with outputs.

```yaml
apiVersion: saturn.flared.io/v1alpha1
kind: SaturnJobDefinition
metadata:
  name: some-job-definition
spec:
  minimalInterval: "@hourly"
  template:
    input:
      default:
         inventory: some-inventory
      paged:
         topic: pages-topic
    output:
      some-output:
        - topic: some-output-topic
      page-output:
        - topic: pages-topic
    pipeline:
      name: ...
```

At the moment, when an input completes, the job also completes. A lot of changes would be required to
correctly handle multiple inputs that can complete at different times or even fail.

The easiest way to support multiple inputs would therefore be to support multiple
inputs in `JobSpec` and to return multiple Job(Definition)s when compiling the static definitions.

Multiple queues/jobs would still be created, but the topologies would be easier to maintain.
