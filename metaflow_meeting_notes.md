# Metaflow Meeting Notes

## Questions:

### Justin:

#### Subprocess/System Calls
- Flows within flows?
  - Subflows (composition) are planned
- Flow from python without system call?
  - There a plan for this (see design docs)
- Use of subprocess internally
  - For bash scripts, etc. Easy multithreading?
  - Only when necessary, but it's necessary often

#### Stitching together 2 workflows
- Getting data: 
  - last_successful_run is implicit output of upstream run
  - tags: get me the latest with this attribute

### Isaac:

#### What pipeline tools work with metaflow?
- Metaflow can compile local files into an orchestrator format
  - Orchestrators being airflow, argo, etc.
  - Metaflow fills some gaps in development for orchestrators

### Nick:

#### Is this right?
Are we using metaflow right? Is this (data artifact organization back-end with emphasis on provenance) what it's for? Should we be trying to hide metaflow as much as possible?

- Sure
- Metaflow UI / @project might be useful

#### Subclassing Flowspec for our own features

- Also consider own decorators
  - Can intercept different parts of metaflow lifecycle
  - Many customers have their own stuff that override metaflow stuff as needed

#### How do people use tags?

- Further organizing data
- For CI/CD processes
- Catalog
- Artifacts are a fact (doesn't change), tags are opinions (change over time)

https://outerbounds.com/blog/five-ways-to-use-the-new-metaflow-tags 

### Tyler:

#### Highlights, double-check knowledge

- Metaflow is base-level tool for organizing ETL units of work
  - ML ops requires a lot of infrastructure, Metaflow is a part of that
  - But as needs and organization grows, other things need to come in
  - In a car, Metaflow is the steering wheel and dashboard
- Increasingly becoming more capable as a standalone tool
  - Scaling up requires ETL automation/orchestration right now


## Resources

### Design Docs
- https://docs.google.com/document/d/1HJW9TH6lHEUojqDTzfgJZJXgjg3xZr43-p8_vjYJM1c/edit 
- https://docs.google.com/document/d/1liTvpACWKioCSQTUv5iO3g2AKuLu4x3EYFwEl43WAZU/edit#heading=h.lzd4f7btdmb6

### Issues
- https://github.com/Netflix/metaflow/issues/424 

### Documentation Pages
- https://docs.metaflow.org/going-to-production-with-metaflow/coordinating-larger-metaflow-projects 