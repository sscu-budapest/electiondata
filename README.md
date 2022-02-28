# Research Project Template - WIP

> relies on [sscutils](https://github.com/sscu-budapest/sscutils) and the [tooling](https://sscu-budapest.github.io/tooling) of sscub

### To create a new project

- install requirements `pip install -r requirements`
- add datasets that use the same template to `imported-namespaces.yaml`
- import them using `inv import-namespaces` and `inv load-external-data`
  - ensure that you import envs that are served from sources you have access to
- create, register and document steps in a pipeline you will run on the data
- run pipeline steps using invoke `inv run`

### config files

### metadata

- `imported-namespaces.yaml`

## Test projects

TODO: document dogshow and everything else much better here