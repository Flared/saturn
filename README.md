# saturn

Saturn is a job scheduling and data processing system developed for web crawling needs at Flare Systems.

## Documentation

- [**Architecture Overview**](docs/architecture_overview.md)

## Development

Install [nox](https://nox.thea.codes/en/stable/) and [poetry](https://python-poetry.org/docs/).

To run all tests: `nox`
To format code: `nox -rs format`

You can also work from the shell with:

```console
$ # Install the project locally.
$ poetry install
$ poetry shell
$ # Run the utilities.
$ py.test tests -xsvv
$ mypy src tests
```
