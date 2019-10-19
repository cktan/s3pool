# S3Pool &mdash; a S3 cache on local disk

## Synopsis

    % ### create a dir to store cache files and start the service
    % mkdir s3cache
    % s3pool -p 9999 -D s3cache &

    % ### download a file 
    % echo '["PULL", "bucketname", "path/to/a/file/on/s3.txt"]' | nc localhost 9999
    OK
    /abs/path/to/the/file/on/local/disk.txt
    % cat /abs/path/to/the/file/on/local/disk.txt
    

## Top Features

+ All AWS operations are performed using the [AWS Command Line
Interface](https://aws.amazon.com/cli/) program. This means
authentication and authorization use the credentials stored in
`~/.aws/` of the user.

+ PULL operation will download file only if it has been modified since
the last download. 

+ Least-recently-used cache files will be deleted when disk space
utilization is above 90%.

## How to build

    % export GOPATH=$(pwd)
    % (cd src/s3pool && go get . && go install)
    % ls bin/s3pool

