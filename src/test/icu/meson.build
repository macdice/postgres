tests += {
  'name': 'icu',
  'sd': meson.current_source_dir(),
  'bd': meson.current_build_dir(),
  'tap': {
    'tests': [
      't/010_database.pl',
      't/020_multiversion.pl',
    ],
    'env': {'with_icu': icu.found() ? 'yes' : 'no'},
  },
}
