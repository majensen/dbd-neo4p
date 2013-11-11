use Module::Build;
use strict;
use warnings;

my $build = Module::Build->new(
  'dist_name' => 'DBD::Neo4p',
  'dist_abstract' => 'A DBI driver for Neo4j via REST::Neo4p',
  'dist_version_from' => 'lib/DBD/Neo4p.pm',
  'config_requires' => {
    'Module::Build' => 0
   },
  'build_requires' => {
    'REST::Neo4p' => '0.212',
    'Test::More' => 0,
    'Test::Exception' => 0
   },
  'requires' => {
    'REST::Neo4p' => '0.212'
   },
  perl => 5.010
);
$build->notes( test_server => $build->prompt("Test server?", "http://127.0.0.1:7474"));
$build->notes( user => $build->prompt("Username?", ""))
$build->notes( pass => $build->prompt("Pass?",""));
$build->create_build_script;
