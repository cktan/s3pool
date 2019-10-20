#ifndef S3POOL_H
#define S3POOL_H

#ifdef __cplusplus
#define EXTERN extern "C"
#else
#define EXTERN extern
#endif

typedef struct s3pool_t s3pool_t;

/**
 *  PULL a file from S3 to local disk. Returns a unix file descriptor
 *  of the file that can be used to read the file or -1 on error.
 */
EXTERN int s3pool_pull(int port, const char* bucket, const char* key,
					   char* errmsg, int errmsgsz);


/**
 *  PUSH a file from local disk to S3. Returns 0 on success, -1 otherwise.
 */
EXTERN int s3pool_push(int port, const char* bucket, const char* key, const char* fpath,
					   char* errmsg, int errmsgsz);

/**
 *  REFRESH a bucket list. Returns 0 on success, -1 otherwise.
 */
EXTERN int s3pool_refresh(int port, const char* bucket,
						  char* errmsg, int errmsgsz);


#endif
