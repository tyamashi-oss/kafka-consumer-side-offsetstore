#!/bin/bash

mvn clean jacoco:prepare-agent test jacoco:report
