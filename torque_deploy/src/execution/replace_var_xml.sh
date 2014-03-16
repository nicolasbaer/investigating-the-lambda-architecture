#!/bin/bash

# replaces all variables in the given file with the environment variables of the current bash and writes it to the output file
# parameters:
#   - input file
#   - output file
#   - variable name to replace
#   - value for variable
# val=$(echo $4 | sed 's/\./\\./g')
cat $1 | sed -e "s,\$$3,$4," > $2