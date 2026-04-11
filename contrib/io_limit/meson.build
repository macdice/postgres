# Copyright (c) 2022-2026, PostgreSQL Global Development Group

io_limit_sources = files(
  'io_limit.c',
)

if host_system == 'windows'
  io_limit_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'io_limit',
    '--FILEDESC', 'io_limit - artificially limit asynchronous I/O for tesing',])
endif

io_limit = shared_module('io_limit',
  io_limit_sources,
  kwargs: contrib_mod_args,
)
contrib_targets += io_limit

install_data(
  'io_limit.control',
  kwargs: contrib_data_args,
)

tests += {
  'name': 'io_limit',
  'sd': meson.current_source_dir(),
  'bd': meson.current_build_dir(),
}
