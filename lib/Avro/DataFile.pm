use v6;
use JSON::Tiny;
use Avro::Auxiliary;
use Avro::Encode;
use Avro::Decode;
use Avro::Schema;

package Avro { 
  
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

  enum Encoding <JSON Binary>; 


  #== Enum ==============================
  #   * Codec
  #   -- the codec, used by the writer
  #======================================

  enum Codec <null deflate>;


  #== Class =============================
  #   * DataFileWriter
  #======================================

  class DataFileWriter {

    constant magicbytes = "A A A C";

    has IO::Handle $!handle;
    has Avro::Encoder $!encoder;
    has Avro::Schema $!schema;
    has Codec $!codec;
    has Buf $!syncmark;
    has Buf $!magic;
    has Associative $!header;

    multi method new(IO::Handle :$handle!, Avro::Schema :$schema!, Encoding :$encoding!, 
      Associative :$metadata? = {}, Codec :$codec? = Codec::null) {

      my Avro::Encoder $encoder;
      given $encoding {
        when Encoding::JSON   { $encoder = Avro::JSONEncoder.new() }
        when Encoding::Binary { $encoder = Avro::BinaryEncoder.new() }
      }
      
      self.bless(handle => $handle, schema => $schema, encoder => $encoder,
        metadata => $metadata, codec => $codec );
    }

    submethod BUILD(IO::Handle :$handle!, Avro::Schema :$schema!, Avro::Encoder :$encoder!,
      Associative :$metadata, Codec :$codec) {

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

  class DataFileReader {

    has IO::Handle $!handle;
    has Avro::Decoder $!decoder;

    multi method new(IO::Handle :$handle!, Encoding :$encoding) {
      my Avro::Decoder $decoder; 
      given $encoding {
        when Encoding::JSON   { $decoder = Avro::JSONDecoder.new() }
        when Encoding::Binary { $decoder = Avro::BinaryDecoder.new() }
      }
      self.bless(handle => $handle, decoder => $decoder);
    }

    submethod BUILD(IO::Handle :$handle, Avro::Decoder :$decoder!){
      $!handle = $handle;
      $!decoder = $decoder;
      
    }

  }

}
