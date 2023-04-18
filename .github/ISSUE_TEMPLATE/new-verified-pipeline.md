---
name: New Verified Pipeline
about: This is a spec for new verified pipeline
title: "[pipeline name] verified pipeline"
labels: verified pipeline
assignees: ''

---

# Description

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
- use some pseudocode if you want
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
* [ ] make sure that the Use Cases can be easily executed by the user with the implemented by **providing demonstrations in all the use cases in the `[pipeline_name]_pipeline.py`
* [ ] all the use cases are tested
* [ ] if there are any customizations you must test them, including unit tests for custom code
