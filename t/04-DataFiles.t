use v6;
use Test;
use lib 'lib';
use Avro; 
use Avro::DataFile;

plan 3;

#======================================
# Test Setup
#======================================

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


my Avro::Schema $schema = parse-schema($avro_ex);
my Avro::DataFileWriter $writer; 
my Avro::DataFileReader $reader; 
my IO::Handle $fh;


#======================================
# Test :: File Header Writes & Reads
#======================================

# creates a file
$fh = $path.IO.open(:w);
lives-ok {$writer = Avro::DataFileWriter.new(:handle($fh),:schema($schema),:encoding(Avro::Encoding::Binary))}, "Opened Data File";
$writer.close;
$fh = $path.IO.open(:r);
lives-ok {$reader = Avro::DataFileReader.new(:handle($fh)); }, "Read Empty Data File";
$fh.close;


#======================================
# Test :: write and read data
#======================================

$fh = $path.IO.open(:w);
my %data = "name" => "Alyssa","favorite_number" => 256;
$writer = Avro::DataFileWriter.new(:handle($fh),:schema($schema),:encoding(Avro::Encoding::Binary));
lives-ok { $writer.append(%data); }, "Appended data";
$writer.close;
$fh = $path.IO.open(:r);
my %result;
#$reader = Avro::DataFileReader.new(:handle($fh)); 
#lives-ok { %result = $writer.read(); }, "Data read from file";
#is-deeply %data, %result, "Correct data read";
#$fh.close;

# clean up
unlink $path;

#lives-ok { $writer.append({}) }, "Can append field";


# vim: filetype=perl6
