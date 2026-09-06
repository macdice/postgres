# Copyright (c) 2026, PostgreSQL Global Development Group

test_simplehash_sources = files(
  'test_simplehash.c',
)

if host_system == 'windows'
  test_simplehash_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'test_simplehash',
    '--FILEDESC', 'test_simplehash - test code for src/include/lib/simplehash.h',])
endif

test_simplehash = shared_module('test_simplehash',
  test_simplehash_sources,
  kwargs: pg_test_mod_args,
)
test_install_libs += test_simplehash

test_install_data += files(
  'test_simplehash.control',
  'test_simplehash--1.0.sql',
)

tests += {
  'name': 'test_simplehash',
  'sd': meson.current_source_dir(),
  'bd': meson.current_build_dir(),
  'regress': {
    'sql': [
      'test_simplehash',
    ],
  },
}
