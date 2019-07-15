#!/bin/bash
set -x

echo 'AoE: Wololo'
(cp kafka-logs/data-0/00000000000000000000.log{.deleted,})
(cp kafka-logs/data-1/00000000000000000000.log{.deleted,})
(cp kafka-logs/data-2/00000000000000000000.log{.deleted,})
(cp kafka-logs/data-3/00000000000000000000.log{.deleted,})
(cp kafka-logs/data-4/00000000000000000000.log{.deleted,})
(cp kafka-logs/locations-0/00000000000000000000.log{.deleted,})

touch data/kafkadump.log
touch data/inputdata.dat
echo -n "" > data/kafkadump.log
echo -n "" > data/inputdata.dat

/opt/kafka/bin/kafka-run-class.sh kafka.tools.DumpLogSegments --print-data-log --deep-iteration --files kafka-logs/data-0/00000000000000000000.log >> data/kafkadump.log
/opt/kafka/bin/kafka-run-class.sh kafka.tools.DumpLogSegments --print-data-log --deep-iteration --files kafka-logs/data-1/00000000000000000000.log >> data/kafkadump.log
/opt/kafka/bin/kafka-run-class.sh kafka.tools.DumpLogSegments --print-data-log --deep-iteration --files kafka-logs/data-2/00000000000000000000.log >> data/kafkadump.log
/opt/kafka/bin/kafka-run-class.sh kafka.tools.DumpLogSegments --print-data-log --deep-iteration --files kafka-logs/data-3/00000000000000000000.log >> data/kafkadump.log
/opt/kafka/bin/kafka-run-class.sh kafka.tools.DumpLogSegments --print-data-log --deep-iteration --files kafka-logs/data-4/00000000000000000000.log >> data/kafkadump.log
/opt/kafka/bin/kafka-run-class.sh kafka.tools.DumpLogSegments --print-data-log --deep-iteration --files kafka-logs/locations-0/00000000000000000000.log > data/locationdump.log

awk '{ if ($4=="CreateTime:") {data=$5","$15; print data} }' data/kafkadump.log > data/inputdata.dat
awk '{ if ($4=="CreateTime:") {data=$5","$15","$17; print data} }' data/locationdump.log > data/location.dat
