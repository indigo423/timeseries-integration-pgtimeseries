##
# Makefile to build OpenNMS from source
##
.DEFAULT_GOAL := compile

SHELL                 := /bin/bash -o nounset -o pipefail -o errexit
WORKING_DIRECTORY     := $(shell pwd)
ARTIFACTS_DIR         := target/artifacts
MAVEN_ARGS            := --batch-mode -DupdatePolicy=never -Djava.awt.headless=true -Dstyle.color=always

GIT_BRANCH            := $(shell git branch | grep \* | cut -d' ' -f2)
OPENNMS_HOME          := /opt/opennms
OPENNMS_VERSION       := $(shell mvn help:evaluate -Dexpression=project.version -q -DforceStdout)
VERSION               := $(shell echo ${OPENNMS_VERSION} | sed -e 's,-SNAPSHOT,,')
RELEASE_BUILD_KEY     := onms
RELEASE_BRANCH        := $(shell echo ${GIT_BRANCH} | sed -e 's,/,-,g')
RELEASE_COMMIT        := $(shell git rev-parse --short HEAD)

.PHONY: help
help:
	@echo ""
	@echo "Makefile to build artifacts for OpenNMS"
	@echo ""
	@echo "Requirements to build:"
	@echo "  * OpenJDK 17 Development Kit"
	@echo "  * Maven"
	@echo "  * Docker"
	@echo "We are using the command tool to test for the requirements in your search path."
	@echo ""
	@echo "Build targets:"
	@echo "  help:                  Show this help"
	@echo "  validate:              Fail quickly by checking project structure with mvn:clean"
	@echo "  compile:               Compile OpenNMS from source code with runs expensive tasks doing"
	@echo "  assemble:              Assemble the build artifacts with expensive tasks for a production build"
	@echo "  unit-tests:            Run full unit test suite"
	@echo "  integration-tests:     Run full integration test suit"
	@echo "  clean:                 Clean assembly and docs and mostly used to recompile or rebuild from source"
	@echo ""
	@echo ""

.PHONY: show-info
show-info:
	@echo "MAVEN_OPTS=\"$(MAVEN_OPTS)\""
	@echo "MAVEN_ARGS=\"$(MAVEN_ARGS)\""
	@mvn $(MAVEN_ARGS) --version

.PHONY: deps-build show-info
deps-build:
	@echo "Check build dependencies: Java JDK, Maven, Docker"
	command -v java
	command -v javac
	command -v mvn
	command -v docker
	mkdir -p $(ARTIFACTS_DIR)

.PHONY: validate
validate: deps-build
	mvn $(MAVEN_ARGS) clean

.PHONY: compile
compile: deps-build
	mvn $(MAVEN_ARGS) install -DskipTests=true -DskipITs=true 2>&1 | tee $(ARTIFACTS_DIR)/mvn.compile.log

.PHONY: tests
tests: deps-build compile
	mvn $(MAVEN_ARGS) -DskipITs=false -DskipTests=false install test integration-test

.PHONY: clean
clean: deps-build
	mvn $(MAVEN_ARGS) clean

.PHONY: libyear
libyear: deps-build
	@echo "Analyze dependency freshness measured in libyear"
	@mkdir -p $(ARTIFACTS_DIR)/logs
	mvn $(MAVEN_ARGS) io.github.mfoo:libyear-maven-plugin:analyze 2>&1 | tee $(ARTIFACTS_DIR)/logs/libyear.log
