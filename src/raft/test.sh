#!/bin/bash
rm out
while [ 1 ]
do
  go test -run TestFigure8Unreliable2C >> out
  #go test -run 2B >> out
  #go test -run 2A >> out
  #go test -run TestFailNoAgree2B >> out
  #go test -run TestBackup2B >> out
  #go test -run TestConcurrentStarts2B >> out
  sleep 1s
  echo "sleep for a while\n" >> out
done
exit 0