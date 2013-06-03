hdfs-cleanup
============

hdfs-cleanup solves the problem of cleaning up /tmp on hadoop in a multi-user environment
script will delete files older than [mtime] hours (default 30 days) from hdfs directory (default /tmp)


Usage
=====
Run:
```
./hdfs-cleanup
```
carefully! inspect the output
run
```
./hdfs-cleanup -delete=true
```

Help
====
```
Usage of ./hdfs-cleanup:
  -delete=false: delete or just print files to delete to STDOUT
  -delete_limit=1000: delete [delete_limit] files at once
  -mtime=720: mtime of files to delete in hours
  -namenode="localhost": namenode address
  -port="50070": namenode port
  -prefix="/tmp": prefix to cleanup
```
