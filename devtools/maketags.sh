#!/bin/sh
find ../ -name "*[cht]" > ./cscope.files
ctags --extra=+q -L ./cscope.files -f ./tags
rm ./cscope.files
