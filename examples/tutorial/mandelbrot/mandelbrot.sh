#!/bin/bash

UUID=$(cat /proc/sys/kernel/random/uuid)

curl -O https://pypi.python.org/packages/source/v/virtualenv/virtualenv-1.10.tar.gz
tar xvfz virtualenv-1.10.tar.gz
python virtualenv-1.10/virtualenv.py /tmp/sagaenv-$UUID

. /tmp/sagaenv-$UUID/bin/activate

pip install PIL

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

python $DIR/mandelbrot.py $@
