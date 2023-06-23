#!/bin/sh
cat "$1" | grep 'Europe' > "$2" # filter to only European countries
