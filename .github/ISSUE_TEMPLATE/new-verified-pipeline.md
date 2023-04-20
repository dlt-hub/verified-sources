---
name: New verified pipeline
about: I want to create a new verified pipeline
title: "[pipeline name] verified pipeline"
labels: verified pipeline
assignees: ''

---

# Planned Pipeline Info
Please provide info below when opening the issue to give use heads-up on what is planned.
- **name** of the pipeline to be used by `dlt init` and to be placed in `pipelines` folder: [e.g. pipedrive]
- **category and description** of the pipeline: [e.g. CRM, loads the relevant data from pipedrive api]

You can fill the data below when writing spec.

# Use Cases
Please provide descriptions up to 3 most important use cases that users of this pipeline do. Those use cases will be:
- implemented
- reviewed
- demonstrated in pipeline script
- documented
use case description is not only targeted at the developers but also by people creating test account and doing demos.

# Sources / Resources / Endpoints
Define pipeline interface to the user in terms of sources and resources.
- enumerate all the sources with information form which endpoints the data comes from
- ideally provide the config arguments to each source (ie. start dates, report ranges etc.)
- you can use pseudocode to show how you intend to use the source
- provide the default write disposition for resources in the source (all append, all replace?)
- in the sources identify the incremental and merge resources and specify them in a reasonable way (ideally by giving the cursor columns - what is the last value really?, primary keys and merge keys).

# Customization
Enumerate everything that goes beyond standard `dlt` building blocks. Suggest the implementation
- use of state
- in the code or as additional transform, filter or map function
- ask dlt team for help if it looks like complex software task

# Test account / test data
- tell what data you expect in the test dataset. otherwise refer to use cases

# Implementation tasks
Below you have a proposal for implementation tasks
* [ ] implement all the described sources, resources and endpoints
* [ ] make sure that the Use Cases can be easily executed by the pipeline's user by **providing demonstrations of all the use cases** in the `[pipeline_name]_pipeline.py`
* [ ] all the use cases are tested
* [ ] if there are any customizations you must test them, including unit tests for custom code
