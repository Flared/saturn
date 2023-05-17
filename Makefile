.all: nox

.DEFAULT_GOAL := .all

.venv-tools:
	rm -rf venv
	python -m venv .venv-tools
	.venv-tools/bin/pip install poetry==1.4.2 nox==2023.4.22 nox-poetry==1.0.2 pip==23.1.2 || (rm -rf venv && exit 1)

.PHONY: nox
nox: .venv-tools
	bash -c "\
		source .venv-tools/bin/activate \
		&& .venv-tools/bin/nox \
	"

.PHONY: nox-tests
nox-tests: .venv-tools
	bash -c "\
		source .venv-tools/bin/activate \
		&& .venv-tools/bin/nox -s tests \
	"

.PHONY: nox-example-tests
nox-example-tests: .venv-tools
	bash -c "\
		source .venv-tools/bin/activate \
		&& .venv-tools/bin/nox -s example_tests \
	"

.PHONY: nox-mypy
nox-mypy: .venv-tools
	bash -c "\
		source .venv-tools/bin/activate \
		&& .venv-tools/bin/nox -s mypy \
	"

.PHONY: nox-lint
nox-lint: .venv-tools
	bash -c "\
		source .venv-tools/bin/activate \
		&& .venv-tools/bin/nox -s lint \
	"

.PHONY: nox-format
nox-format: .venv-tools
	bash -c "\
		source .venv-tools/bin/activate \
		&& .venv-tools/bin/nox -s format \
	"

.PHONY: clean
clean:
	rm -rf .venv-tools
	rm -rf .nox
	rm -rf dist
