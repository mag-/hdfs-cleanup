hdfs-cleanup
============

hdfs-cleanup script to delete files older than [duration] from hdfs directory ( default to /tmp )

Usage
=====
Run:
./hdfs-cleanup

carefully! inspect the output
run
./hdfs-cleanup -delete=true

Help
====
Usage of ./hdfs-cleanup:
  -delete=false: delete or just print files to delete to STDOUT
  -delete_limit=1000: delete [delete_limit] files at once
  -duration=720: mtime in hours
  -namenode="localhost": namenode address
  -port="50070": namenode port
  -prefix="/tmp": prefix to cleanup


