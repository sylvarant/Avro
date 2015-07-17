use v6;
use JSON::Tiny;
use Avro::Auxiliary;
use Avro::Schema;

package Avro{

  #======================================
  # Exceptions
  #======================================

  class X::Avro::EncodeFail is Avro::AvroException {
    has Avro::Schema $.schema;
    has Mu $.data;
    method message { "Failed to encode "~$!data~" as "~$!schema.type() }
  }


  #== Role ==============================
  #   * Encoder
  #======================================

  role Encoder {
    method encode(Avro::Schema, Mu:D --> Blob) { * };
  }


  #== Class =============================
  #   * BinaryEncoder
  #======================================

  class BinaryEncoder does Encoder { 

    # integers and longs are encode as variable sized zigzag numbers
    sub encode_long(int $l){
      my @var_int = to_varint(to_zigzag($l));  
      my Str $template = ((1..@var_int.elems()).map:{ "C" }).join(" ");
      pack($template,@var_int);
    }


    multi submethod encode_schema(Avro::Array $schema, IO::Handle $handle, Positional:D $arr) { * }   

    multi submethod encode_schema(Avro::Map $schema, IO::Handle $handle, Associative:D $hash) { * } 

    multi submethod encode_schema(Avro::Enum $schema, IO::Handle $handle, Str:D $str) { * } 

    multi submethod encode_schema(Avro::Union $schema, IO::Handle $handle, Mu:D $data) { * }

    multi submethod encode_schema(Avro::Fixed $schema, IO::Handle $handle, Mu:D $data) { * }

    multi submethod encode_schema(Avro::Null $schema, IO::Handle $handle, Mu:U $any) { 
      $handle.write(pack("C",0)); 
    }

    multi submethod encode_schema(Avro::String $schema, IO::Handle $handle, Str:D $str) { * }

    multi submethod encode_schema(Avro::Bytes $schema, IO::Handle $handle, Str:D $str) { * }

    multi submethod encode_schema(Avro::Boolean $schema, IO::Handle $handle, Bool:D $bool) {  
      $handle.write(pack("C",$bool));
    }   

    multi submethod encode_schema(Avro::Integer $schema, IO::Handle $handle, Int:D $int) {
      $handle.write(encode_long($int)); 
    }

    multi submethod encode_schema(Avro::Long $schema, IO::Handle $handle, Int:D $long) {  
      $handle.write(encode_long($long)); 
    }

    multi submethod encode_schema(Avro::Record $schema, IO::Handle $handle, Associative:D $hash) { * }   

    multi submethod encode_schema(Avro::Float $schema, IO::Handle $handle, Rat:D $float) { * }

    multi submethod encode_schema(Avro::Double $schema, IO::Handle $handle, Rat:D $double) { * }

    method encode(Avro::Schema $schema, IO::Handle $handle, Mu $data) {  
      # TODO check handle?
      try {
      #  say $schema.WHAT.gist();
        self.encode_schema($schema,$handle,$data); 
      }
      CATCH { default { say $_; X::Avro::EncodeFail.new(:schema($schema),:data($data)).throw() }}
    };
    
  
  };


  #== Class =============================
  #   * JSONEncoder
  #======================================

  class JSONEncoder does Encoder {
  
  
  };

}

