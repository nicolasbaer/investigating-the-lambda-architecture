#!/bin/bash

# parameters:
#   - input file (same as output)
#   - variable name to replace
#   - value for variable
sed -ie "s,\$$2,$3," $1