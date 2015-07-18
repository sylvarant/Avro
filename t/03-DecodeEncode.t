use v6;
use Test;
use lib 'lib';
use Avro; 


#======================================
# Test Setup
#======================================

my $path = 'testing';

my $encoder = Avro::BinaryEncoder.new();
my $decoder = Avro::BinaryDecoder.new();

my @schemas = ( Avro::Boolean.new(), Avro::Boolean.new() , Avro::Null.new(), Avro::Integer.new(), 
  Avro::Integer.new(), Avro::Long.new(), Avro::Bytes.new(), Avro::String.new(), Avro::String.new(),
  Avro::Float.new());
my @datas = ( True, False, Any, 56, -668, (1 +< 60),"00FF","Hello Me", 'møp', 2.5);
my @zipped = (@schemas Z @datas);

plan +@schemas;

#======================================
# decode and encode
#======================================
for @zipped -> $schema, $data {
  my $write = $path.IO.open(:w);
  $encoder.encode($schema,$write,$data);
  $write.close();
  my $read = $path.IO.open(:r);
  is-deeply $decoder.decode($schema,$read),$data,("Data: "~ ($data.gist() ~~ Any.gist() ?? "Nothing" !! $data.Str)  ~
    " correctly encoded & decoded as: "~$schema.type());
  $read.close();
  unlink $path;
}


# vim: filetype=perl6
