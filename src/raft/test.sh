#!/bin/bash
while [ 1 ]
do
  go test -run 2B >> out
  sleep 1s
  echo "sleep for a while\n"
done
exit 0