all:
	$(MAKE) -C src/s3pool all
	$(MAKE) -C client/c all

install: 
	$(MAKE) -C src/s3pool install
	$(MAKE) -C client/c install

clean:
	$(MAKE) -C src/s3pool clean
	$(MAKE) -C client/c clean

