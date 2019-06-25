# Canarying MQL

We make use of the API Developer Canary pipeline to verify MQL functionality. We will achieve this by pushing our changes as a snapshot, building and deploying an API Canary and then running our own MQL/MRE Automated Canary Analysis configuration against the API Canary.

## Building

1. You'll want to deploy changes to a snapshot build of MQL and then a snapshot build of MRE. Both have the `nebula.release` plugin and can be deployed as snapshots using the `./gradlew devSnapshot` command. This functions locally if need be.

1. Push your changes into an [API branch](https://stash.corp.netflix.com/projects/EDGE/repos/server/browse) -- usually by updating the dependency to your snapshot build. Name the branch `username/change-description`

1. Next run the [API Spinnaker Pipeline](https://www.spinnaker.mgmt.netflix.net/#/applications/api/executions) called "Developer Pipeline".
You'll need to specify your branch name, and set the status to snapshot. This will crease an API build with your changes.


## Canary
You'll want to run the canary next via the "Main Developer Canary" pipeline.
Make sure to select the correct build to ensure that your code is running.

This will spin up a Canary, and ensure that it runs API's Automated Canary Analysis (ACA)
configuration. This will verify that our changes did not break anything on the API team's expectations
but it does not explicitly verify MRE behavior.

We can locate the mantis-realtime-events canary configuration at:
[MQL ACA Configurations](https://www.spinnaker.mgmt.netflix.net/#/applications/mql/canary/report).

Click "Start Manual Analysis" and scope the analysis and make sure to scope it correctly.
This will ensure the MRE component of the system is functioning within our bounds.
