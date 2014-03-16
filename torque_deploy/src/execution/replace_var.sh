#!/bin/bash

# replaces all variables in the given file with the variables of the current bash and writes it to the output file
# parameters:
#   - input file
#   - output file

eval echo "\"$(cat <<EOF_$RANDOM
$(<$1)
EOF_$RANDOM
)\"" > $2