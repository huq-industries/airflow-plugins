# Airflow Plugins
Collection of custom airflow plugins made to make our life easier


## Intro

This repository is a community driven custom Airflow plugins collection.
We can keep here interesting plugins until (if ever) they will get incorporated into Airflow code base.


## Chains Plugin

This operators are wrappers around existing operators to create an atomic airflow tasks composed by action deformed by a single operator.
This is especially useful when you have thousands of short tasks where where the overhead is comparable, if not even higher, to the time necessary to perform the task actions.

To use the chains plugin just copy `chains.py` into airflow plugins directory and import the operators from `airflow.operators.chains`.

### BigQueryChainOperator

The `BigQueryChainOperator` chains multiple queries in a single task.
Queries are executed sequentially with a lazy retry in case of failure. 
This operator succeed if and only if all the queries in the chain succeed.


### BigQueryToCloudStorageChainOperator

The `BigQueryToCloudStorageChainOperator` chains multiple table export in a single task.
Exports are executed sequentially with a lazy retry in case of failure. 
This operator succeed if and only if all the exports in the chain succeed.
