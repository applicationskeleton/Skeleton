#!/bin/bash

path=`which task`

if [[ $path == "" ]]; then
    echo "The path of task is not exported"
    echo "Please run:"
    echo 'export PATH=$PATH:/path-to-task'
    exit 1
fi

echo -e "localhost\ttask\t${path}\tINSTALLED\tINTEL32::LINUX\tnull"
