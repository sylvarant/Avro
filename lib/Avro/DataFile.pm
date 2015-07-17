use v6;
use JSON::Tiny;
use Avro::Auxiliary;
use Avro::Encode;
use Avro::Decode;
use Avro::Schema;

package Avro { # not being detected by perl6 at the moment, dear lord
  
  #======================================
  #   Package variables
  #======================================

  constant $schema_h = parse(
    {"type"=> "record", "name"=> "org.apache.avro.file.Header",
     "fields" => [
      {"name"=> "magic", "type"=> {"type"=> "fixed", "name"=> "Magic", "size"=> 4}},
      {"name"=> "meta", "type"=> {"type"=> "map", "values"=> "bytes"}},
      {"name"=> "sync", "type"=> {"type"=> "fixed", "name"=> "Sync", "size"=> 16}}]});


  #== Enum ==============================
  #   * Encoding
  #   -- the output type, used by the 
  #   constructors of reader and writer
  #======================================

  enum Avro::Encoding <JSON Binary>; #perl6 is bugging on naked names right now


  #== Enum ==============================
  #   * Codec
  #   -- the codec, used by the writer
  #======================================

  enum Avro::Codec <null deflate>;


  #== Class =============================
  #   * DataFileWriter
  #======================================

  class Avro::DataFileWriter {

    constant magicbytes = "A A A C";

    has IO::Handle $!handle;
    has Avro::Encoder $!encoder;
    has Avro::Schema $!schema;
    has Avro::Codec $!codec;
    has Buf $!syncmark;
    has Buf $!magic;
    has Associative $!header;

    multi method new(IO::Handle :$handle!, Avro::Schema :$schema!, Avro::Encoding :$encoding!, 
      Associative :$metadata? = {}, Avro::Codec :$codec? = Avro::Codec::null) {

      my Avro::Encoder $encoder;
      given $encoding {
        when Avro::Encoding::JSON   { $encoder = Avro::JSONEncoder.new() }
        when Avro::Encoding::Binary { $encoder = Avro::BinaryEncoder.new() }
      }
      
      self.bless(handle => $handle, schema => $schema, encoder => $encoder,
        metadata => $metadata, codec => $codec );
    }

    submethod BUILD(IO::Handle :$handle!, Avro::Schema :$schema!, Avro::Encoder :$encoder!,
      Associative :$metadata, Avro::Codec :$codec) {

      my @rands = (0..256); # byte range
      my @range = (1..16);
      my $template = (@range.map:{ "C" }).join(" ");
      $!syncmark = pack($template,@range.map:{ @rands.pick(1).end() });
      $!magic = pack(magicbytes,"O","b","j",1);
      $!handle = $handle;
      $!schema = $schema;
      $!encoder = $encoder;
      $!header = { magic => $!magic, sync => $!syncmark,
        meta => { $metadata, 'avro.schema' => $schema.native(), 'avro.codec' => ~$codec,}};
      #IO.print($!handle,$!encoder.encode($schema_h,$!header)); #todo switch based on encoding ?
    }

  }


  #== Class =============================
  #   * DataFileReader
  #======================================

  class Avro::DataFileReader {

    has IO::Handle $!handle;
    has Avro::Decoder $!decoder;

    multi method new(IO::Handle :$handle!, Avro::Encoding :$encoding) {
      my Avro::Decoder $decoder; 
      given $encoding {
        when Avro::Encoding::JSON   { $decoder = Avro::JSONDecoder.new() }
        when Avro::Encoding::Binary { $decoder = Avro::BinaryDecoder.new() }
      }
      self.bless(handle => $handle, decoder => $decoder);
    }

    submethod BUILD(IO::Handle :$handle, Avro::Decoder :$decoder!){
      $!handle = $handle;
      $!decoder = $decoder;
      
    }

  }

}
