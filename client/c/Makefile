CFILES = s3pool.c
OBJS = $(CFILES:.c=.o)
EXECS = s3cat
LIBS = libs3pool.a


# to compile for debug: make DEBUG=1
# to compile for no debug: make
ifdef DEBUG
    CFLAGS += -O0 -g
else
    CFLAGS += -O2 -DNDEBUG
endif

CFLAGS += -std=c99 -Wall -Wextra

.PHONY: all
all: $(LIBS) $(EXECS)


libs3pool.a: $(OBJS)
	ar -rcs $@ $^

s3pool.o: s3pool.c 


s3cat: s3cat.c $(LIBS)


.PHONY: clean
clean:
	rm -f $(OBJS) $(EXECS) $(LIBS)
