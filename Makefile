PYTHON=`which python`
DESTDIR=/
PROJECT=cassobjects
BUILDIR=$(CURDIR)/debian/$(PROJECT)
VERSION=0.0.1

all:
	@echo "make source - Create source package"
	@echo "make install - Install on local system"
	@echo "make deb - Generate a deb package"
	@echo "make clean - Get rid of scratch and byte files"

source:
	$(PYTHON) setup.py sdist $(COMPILE)

install:
	$(PYTHON) setup.py install --root $(DESTDIR) $(COMPILE)

deb:
	# build the source package in the parent directory
	# then rename it to project_version.orig.tar.gz
	$(PYTHON) setup.py sdist $(COMPILE) --dist-dir=../
	rename -f 's/$(PROJECT)-(.*)\.tar\.gz/$(PROJECT)_$$1\.orig\.tar\.gz/' ../*
	# build the package
	pdebuild --pbuilder cowbuilder

clean:
	$(PYTHON) setup.py clean
	rm -rf build/ MANIFEST .pc/ debian/$(PROJECT)/ debian/*.debhelper debian/*.substvars debian/*.log debian/files debian/patches/ src/*.egg-info
	find . -name '*.pyc' -delete
