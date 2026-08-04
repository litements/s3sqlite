SHELL := bash
.SHELLFLAGS := -eu -o pipefail -c
MAKEFLAGS += --warn-undefined-variables
MAKEFLAGS += --no-builtin-rules

.DEFAULT_GOAL := help
.PHONY: help
help: ## Display this message
	@grep -E \
		'^[a-zA-Z\.\$$/]+.*:.*?##\s.*$$' $(MAKEFILE_LIST) | \
		sort | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-38s\033[0m %s\n", $$1, $$2}'

.PHONY: install
install: ## Create the environment and install development dependencies
	$(MAKE) sync

.PHONY: lock
lock: ## Update uv.lock from the declared dependency ranges
	uv lock

.PHONY: lock-upgrade
lock-upgrade: ## Upgrade all dependencies within the declared ranges
	uv lock --upgrade

.PHONY: sync
sync: ## Synchronize the uv environment with the lockfile
	uv sync --group dev

.PHONY: dist
dist: ## Build source and wheel distributions
	uv build --no-sources

.PHONY: clean
clean: ## Clean artifacts
	@rm -rf build/ dist/ src/*.egg-info/

.PHONY: test
test: ## Run the complete test suite
	uv run --group dev pytest

.PHONY: test-docker
test-docker: ## Run the Garage integration test suite
	uv run --group dev pytest tests/test_s3sqlite.py -vv -x

.PHONY: typecheck
typecheck: ## Run static type checks
	uv run --group dev mypy .

.PHONY: lint
lint: ## Run lint checks
	uv run --group dev ruff check .

.PHONY: format
format: ## Format Python files
	uv run --group dev ruff format .

.PHONY: format-check
format-check: ## Check Python formatting without changing files
	uv run --group dev ruff format --check .

.PHONY: bump
bump: ## Bump the version number
	uv version --bump minor

.PHONY: publish
publish: ## Test, build, and publish to PyPI
	@: "$${UV_PUBLISH_TOKEN:?Set UV_PUBLISH_TOKEN before publishing}"
	$(MAKE) test
	@rm -rf dist/
	uv build --no-sources
	uv publish

tag: _TAG := $${TAG:?'FAIL. TAG variable not set'}
tag:
	git tag $(_TAG)
	git push --atomic --set-upstream origin main $(_TAG)
