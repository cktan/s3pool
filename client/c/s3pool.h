#ifndef S3POOL_H
#define S3POOL_H

#ifdef __cplusplus
#define EXTERN extern "C"
#else
#define EXTERN extern
#endif

typedef struct s3pool_t s3pool_t;

/**

   PULL a file from S3 to local disk. 
 
   On success, return the path to the file pulled down from S3. Caller
   must free() the pointer returned. 
 
   On failure, return a NULL ptr.

 */
EXTERN char* s3pool_pull(int port, const char* bucket, const char* key,
						 char* errmsg, int errmsgsz);


/**
 *  PUSH a file from local disk to S3. Returns 0 on success, -1 otherwise.
 */
EXTERN int s3pool_push(int port, const char* bucket, const char* key, const char* fpath,
					   char* errmsg, int errmsgsz);


/**

   GLOB file names in a bucket. 
 
   On success, return a buffer containing strings terminated by
   NEWLINE. Each string is a path name in the S3 bucket that matched
   pattern. Caller must free() the buffer returned.
 
   On failure, return a NULL ptr.

*/
EXTERN char* s3pool_glob(int port, const char* bucket, const char* pattern,
						 char* errmsg, int errmsgsz);

/**
 *  REFRESH a bucket list. Returns 0 on success, -1 otherwise.
 */
EXTERN int s3pool_refresh(int port, const char* bucket,
						  char* errmsg, int errmsgsz);


#endif
