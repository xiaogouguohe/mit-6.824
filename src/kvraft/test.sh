#!/bin/bash
rm out
while [ 1 ]
do
  #go test -run 3A >> out
  #go test -run TestBasic3A >> out
  #go test -run TestConcurrent3A >> out
  #go test -run TestUnreliable3A >> out
  #go test -run TestUnreliableOneKey3A >> out
  #go test -run TestOnePartition3A >> out
  #go test -run TestManyPartitionsOneClient3A >> out
  #go test -run TestManyPartitionsManyClients3A >> out
  #go test -run TestPersistOneClient3A >> out
  #go test -run TestPersistConcurrent3A >> out
  #go test -run TestPersistConcurrentUnreliable3A >> out
  #go test -run TestPersistPartition3A >> out
  #go test -run TestPersistPartitionUnreliableLinearizable3A >> out
  #go test -run TestSnapshotRPC3B >> out
  #go test -run TestSnapshotSize3B >> out
  go test -run TestSnapshotRecover3B >> out
  sleep 1s
  echo "sleep for a while\n" >> out
done
exit 0