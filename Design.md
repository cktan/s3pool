# S3Pool &mdash; a S3 cache on local disk

## License

    S3Pool -- a S3 cache on local disk
    Copyright (c) 2019 CK Tan
    cktanx@gmail.com
  
    S3Pool can be used for free under the GNU General Public License
    version 3, where anything released into public must be open source,
    or under a commercial license. The commercial license does not
    cover derived or ported versions created by third parties under
    GPL. To inquire about commercial license, please send email to
    cktan@gmail.com.

## Usage

    s3pool -p port -D homedir

## HOMEDIR

The executable will chdir into the homedir given on the command line. 
The homedir shall have the following subdirectories:

+ log : where log files reside
+ tmp : where temp files reside
+ data : subdirs in `data/` are BUCKETDIRs, which contain files in
their respective buckets


## BUCKETDIR

A special `__list__` file is maintained in each BUCKETDIR, and is
updated when the REFRESH command is given. It contains a full listing
of all keys in the bucket, delimited by a NEWLINE character. The
DQUOTE char and NEWLINE char are not expected to be part of key names.


## Object Files

S3 Objects of a bucket are stored in its BUCKETDIR, using the slash
character as path separators. Two consequtive slashes in a key name is
not handled.

## Meta Files

For each object file `FNAME`, there is a corresponding meta file named
`FNAME__meta__`. This stores the output of `aws s3api get-object` meta
information that was returned along with the file. Here is a sample:

    {
        "LastModified": "Mon, 14 Oct 2019 19:51:18 GMT",
        "ETag": "\"83839df1582f29ada551f698b39fc3ac\"",
        "ContentLength": 555,
        "ContentType": "text/plain",
        "Metadata": {},
        "AcceptRanges": "bytes"
    }

The ETag entry is used to determine if a file has been modified and
needs to be downloaded from S3.

## Commands

Requests are submitted as JSON array objects that are single-line in form.
Replies consist of a status line followed by payload.  Status line is
either OK or ERROR. In the case of OK, the content is defined by the
command submitted. In the case of ERROR, the content is an error
message pertinent to the error.

### GLOB

Returns a list of keys matching a glob pattern.

SYNTAX: ["GLOB", "bucket", "pattern"]


### REFRESH

Refresh the `__dir__` file of a particular bucket.

Syntax: ["REFRESH", "bucket"]



### PULL 

If the file is cached AND is unchanged on S3, return it.
Otherwise, pull the file from S3.

Syntax: ["PULL", "bucket-name", "key-name"]

The reply is an absolute path in the local filesystem that the user
can use to access the S3 object.


### PUSH 

Push a file to S3.

Syntax: ["PUSH", "bucket", "key", "absolute-path-to-file"]


## Disk Monitor

A watchdog keeps the disk utilization under 90%. Whenever this high
water mark is reached, the watchdog starts to delete files cached in
the `data/` directory using some form of LRU algorithm based on access
time.

## List Monitor

A watchdog keeps the `__list__` file under each bucket up-to-date by
refreshing it every 5 minutes.

