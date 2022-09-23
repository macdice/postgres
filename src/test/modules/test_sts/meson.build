test_sts = shared_module('test_sts',
  ['test_sts.c'],
  kwargs: pg_mod_args,
)
testprep_targets += test_sts

install_data(
  'test_sts.control',
  'test_sts--1.0.sql',
  kwargs: contrib_data_args,
)

tests += {
  'name': 'test_sts',
  'sd': meson.current_source_dir(),
  'bd': meson.current_build_dir(),
  'regress': {
    'sql': [
      'test_sts',
    ],
  },
}
