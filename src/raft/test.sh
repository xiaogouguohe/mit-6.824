#!/bin/bash
rm out
while [ 1 ]
do

  #go test -run 2A >> out
  #go test -run 2B >> out
  #go test -run 2C >> out
  go test >> out
  #go test -run TestFigure8Unreliable2C >> out
  #go test -run  TestPersist12C >> out
  #go test -run TestFailNoAgree2B >> out
  #go test -run TestBackup2B >> out
  #go test -run TestConcurrentStarts2B >> out
  #go test -run TestPersist22C >> out
  #go test -run TestReliableChurn2C >> out
  #go test -run TestUnreliableChurn2C >> out
  sleep 1s
  echo "sleep for a while\n" >> out
done
exit 0