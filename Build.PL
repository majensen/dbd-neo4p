use Module::Build;
use strict;
use warnings;

my $build = Module::Build->new(
  'dist_name' => 'DBD-Neo4p',
  'dist_abstract' => 'DBI-compliant interface for Neo4j server',
  'dist_version_from' => 'lib/DBD/Neo4p.pm',
  'dist_author' => 'Mark A. Jensen',
  license => 'perl',
  perl => '5.010',
  'configure_requires' => {
    'Module::Build' => 0
   },
  'build_requires' => {
    'REST::Neo4p' => '0.2220',
    'Test::More' => 0,
    'Test::Exception' => 0,
    'Module::Build' => 0,
    'DBI' => 0
   },
  'requires' => {
    'REST::Neo4p' => '0.2220',
    'DBI' => 0,
    'JSON' => 0
   },
  perl => 5.010
);
$build->notes( test_server => $build->prompt("Test server?", "http://127.0.0.1:7474"));
$build->notes( user => $build->prompt("Username?", ""));
$build->notes( pass => $build->prompt("Pass?",""));
$build->create_build_script;
