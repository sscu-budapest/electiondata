# Research Project Template - WIP

> relies on [datazimmer](https://github.com/sscu-budapest/datazimmer) and the [tooling](https://sscu-budapest.github.io/tooling) of sscub

### To create a new project

- install requirements `pip install -r requirements`
- set name and other [configuration](TODO) in `zimmer.yaml`
- create, register and document steps in a pipeline you will run ind different [environments](TODO)
- build metadata to exportable and serialized format with `dz build-meta`
  - if you defined importable data from other artifacts in the config, you can import them with `load-external-data` 
  - ensure that you import envs that are served from sources you have access to
- build and run pipeline steps by running `dz run`
- validate that the data matches the [datascript](TODO) description with `dz validate`

## Test projects

TODO: document dogshow and everything else much better here