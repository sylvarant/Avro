use v6;
use Test;
use lib 'lib';
use Avro; 

plan 0;

my $path = "datafile";
my $avro_ex = Q<<{"namespace": "example.avro",
 "type": "record",
 "name": "User",
 "fields": [
     {"name": "name", "type": "string"},
     {"name": "favorite_number",  "type": ["int", "null"]},
     {"name": "favorite_color", "type": ["string", "null"]}
 ]
}>>;

#======================================
# Test :: File Writes
#======================================

my $fh = $path.IO.open(:w);

my Avro::Schema $schema = parse-schema($avro_ex);
my Avro::DataFileWriter $writer = Avro::DataFileWriter.new(:handle($fh),:schema($schema),:encoding(Avro::Encoding::JSON));

#clean up
unlink $path;


# vim: filetype=perl6
