#!/bin/bash
rm out
while [ 1 ]
do
  go test -run 2C >> out
  sleep 1s
  echo "sleep for a while\n" >> out
done
exit 0