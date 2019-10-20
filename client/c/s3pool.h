#ifndef S3POOL_H
#define S3POOL_H

#ifdef __cplusplus
#define EXTERN extern "C"
#else
#define EXTERN extern
#endif

typedef struct s3pool_t s3pool_t;

/**
 *  Start a s3pool conversation
 */
EXTERN s3pool_t* s3pool_connect(int port);

/**
 *  Cleanup the connection
 */
EXTERN void s3pool_close(s3pool_t* handle);


/**
 *  Return last error message
 */
EXTERN const char* s3pool_errmsg(s3pool_t* handle);


/**
 *  PULL a file from S3 to local disk. Returns a unix file descriptor
 *  of the file that can be used to read the file or -1 on error.
 */
EXTERN int s3pool_pull(s3pool_t* handle, const char* bucket, const char* key);


/**
 *  PUSH a file from local disk to S3. Returns 0 on success, -1 otherwise.
 */
EXTERN int s3pool_push(s3pool_t* handle, const char* bucket, const char* key, const char* fpath);

/**
 *  REFRESH a bucket list. Returns 0 on success, -1 otherwise.
 */
EXTERN int s3pool_refresh(s3pool_t* handle, const char* bucket);


#endif
