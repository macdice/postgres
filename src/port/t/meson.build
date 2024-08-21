port_tests = []
port_tests += executable('001_pg_threads',
  ['001_pg_threads.c'],
  dependencies: [frontend_code],
  kwargs: test_bin_args,
)

tests += {
  'name': 'port',
  'sd': meson.current_source_dir(),
  'bd': meson.current_build_dir(),
  'exec_tap': {
    'tests': port_tests,
  },
}
