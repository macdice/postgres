MODULES = emulate_old_ubuntu_c_utf8_strcoll
EXTENSION = emulate_old_ubuntu_c_utf8_strcoll
MODULE_big = emulate_old_ubuntu_c_utf8_strcoll
OBJS = emulate_old_ubuntu_c_utf8_strcoll.o

emulate_old_ubuntu_c_utf8_strcoll.o: codepoint_table.h

codepoint_table.h:
	python3 ./make_c_table.py < all_codepoints_ubuntu1804-C.UTF8.txt > codepoint_table.h

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
