CFILES = s3pool.c
OBJS = $(CFILES:.c=.o)
LIB = libs3pool.a
EXECS = s3cat s3glob s3refresh s3pull s3push


# to compile for debug: make DEBUG=1
# to compile for no debug: make
ifdef DEBUG
    CFLAGS += -O0 -g
else
    CFLAGS += -O2 -DNDEBUG
endif

CFLAGS += -std=c99 -Wall -Wextra

all: $(LIB) $(EXECS)

libs3pool.a: $(OBJS)
	ar -rcs $@ $^

s3cat: s3cat.c $(LIB)

s3glob: s3glob.c $(LIB)

s3push: s3push.c $(LIB)

s3pull: s3pull.c $(LIB)

s3refresh: s3refresh.c $(LIB)

clean:
	rm -f $(OBJS) $(LIB) $(EXECS) 


.PHONY: all install clean
