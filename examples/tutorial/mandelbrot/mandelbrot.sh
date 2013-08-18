#!/bin/bash

curl --insecure -s https://raw.github.com/pypa/virtualenv/master/virtualenv.py | python - /tmp/sagaenv
. /tmp/sagaenv/bin/activate

pip install PIL

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

python $DIR/mandelbrot.py $@
