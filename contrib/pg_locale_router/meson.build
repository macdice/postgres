# Copyright (c) 2022-2026, PostgreSQL Global Development Group

pg_locale_router_sources = files(
  'pg_locale_router.c',
  'pg_locale_router_icu.c',
  'pg_locale_router_libc.c',
)

if host_system == 'windows'
  pg_locale_router_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'pg_locale_router',
    '--FILEDESC', 'pg_locale_router - version aware locales',])
endif

pg_locale_router = shared_module('pg_locale_router',
  pg_locale_router_sources,
  kwargs: contrib_mod_args,
)
contrib_targets += pg_locale_router

install_data(
  'pg_locale_router--1.0.sql',
  'pg_locale_router.control',
  kwargs: contrib_data_args,
)


tests += {
  'name': 'pg_locale_router',
  'sd': meson.current_source_dir(),
  'bd': meson.current_build_dir(),
  'regress': {
    'sql': [
      'heap_locale_router',
    ],
  },
}
