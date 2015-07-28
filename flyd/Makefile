DESTDIR ?=

SHELL = /bin/bash

TEST_OUT = '/tmp/flyd.test.log'

install:
	mkdir -p ${DESTDIR}/etc/tarantool/instances.enabled
	install -p -m 644 src/flyd.lua ${DESTDIR}/etc/tarantool/instances.enabled
	mkdir -p ${DESTDIR}/usr/local/etc/flyd
	install -p -m 644 src/dictionary.lua ${DESTDIR}/usr/local/etc/flyd
	install -p -m 644 src/conf/flyd.conf.example ${DESTDIR}/usr/local/etc/flyd

test:
	tarantool test/flyd.test.lua `whoami` | tee ${TEST_OUT}
	@[[ `grep -cP '^\s+not ok' ${TEST_OUT}` == 0 ]]

.PHONY: test
