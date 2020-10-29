#!/bin/bash
rm out
while [ 1 ]
do
  #go test -run TestBasic3A >> out
  #go test -run TestConcurrent3A >> out
  #go test -run TestUnreliable3A >> out
  #go test -run TestUnreliableOneKey3A >> out
  go test -run TestOnePartition3A >> out
  sleep 1s
  echo "sleep for a while\n" >> out
done
exit 0