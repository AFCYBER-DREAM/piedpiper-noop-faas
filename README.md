# PiedPiper Flake8 Function

### Table of Contents

* [Getting Started](#getting-started)
* [Prerequisites](#prerequisites)
* [Installing](#installing)
* [Inputs and Outputs](#inputs-and-outputs)
* [Running the Tests](#running-the-tests)
* [Contributing](#contributing)
* [Versioning](#versioning)
* [Authors](#authors)
* [License](#license)
* [Acknowledgements](#acknowledgments)


## Getting Started

To deploy this function you must have OpenFaaS installed. To create a development environment see (https://github.com/AFCYBER-DREAM/ansible-collection-pidev)

### Prerequisites

OpenFaaS

### Installing

To install this function on OpenFaaS do the following after authentication:

```
git clone https://github.com/AFCYBER-DREAM/piedpiper-flake8-faas.git
cd piedpiper-flake8-faas
faas build
faas deploy
```

To validate that your function installed correctly you can run the following:

```
faas ls
```

## Inputs and Outputs

This function expects to receive its data via an HTTP POST request. The format of the request should be as follows:

1. A zipfile containing the files to be linted
2. A run_vars.yml file, in the root of the zipfile which looks like the following:

```yaml
ci:
  ci_provider: gitlab-ci
  ci_provider_config: {{ contents of .gitlab-ci.yml }}
file_config:
  - file: test.sh
    linter: noop
  - file: etc
pi_global_vars:
  ci_provider: gitlab-ci
  project_name: {{ project_name }}
  vars_dir: default_vars.d
  version: {{ version }}
pi_lint_pipe_vars:
  run_pipe: True
  url: http://172.17.0.1:8080/function
  version: latest
pi_validate_pipe_vars:
  run_pipe: True
  url: http://172.17.0.1:8080/function
  policy:
    enabled: True
    enforcing: True
    version: 0.0.1
options: None
```

Piedpiper-flake8-faas will take these run_vars.yml and build
a flake8 command based on the `options` dict that is found. It will
then perform a `flake8` and return any stdout and stderr to the user.

## Running the tests

Currently we only have functional tests and linting tests for this
repository. These tests can be run by invoking tox.

```bash
tox -e lint # OR
tox -e functional # OR
tox
```

### Test Prerequisites

Tox must be installed and an OpenFaaS environment must be available locally.
You must also deploy the image to OpenFaaS.

There is an simple bash script which can be used to turn a local machine into
an OpenFaaS development environment. This can be found in `tools/scripts/setup-env.sh`.
This is the script that is being used by Travis-CI to deploy the test environment.

We also have an ansible role available to setup the OpenFaaS environment. This 
can be found [here](https://github.com/AFCYBER-DREAM/ansible-collection-pidev)

## Contributing

Please read [CONTRIBUTING.md](https://github.com/AFCYBER-DREAM/piedpiper-picli) for details on our code of conduct, and the process for submitting pull requests to us.

## Versioning

We use [SemVer](http://semver.org/) for versioning. For the versions available, see the [tags on this repository](https://github.com/piedpiper-flake8-faas/tags).

## Authors

See also the list of [contributors](https://github.com/AFCYBER-DREAM/piedpiper-flake8-faas/contributors) who participated in this project.

## License
MIT