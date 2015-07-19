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

    has Int $!blocksize;

    # constructor
    submethod BUILD( :$blocksize = 250 ) { 
      $!blocksize = $blocksize;
    }

    # int8 template "*" doesn't work as I expect it too
    sub template(int $length) {
      ((1..$length).map:{ "C" }).join(" ");
    }

    # integers and longs are encode as variable sized zigzag numbers
    sub encode_long(int $l){
      my @var_int = to_varint(to_zigzag($l));  
      pack(template(@var_int.elems()),@var_int);
    }

    multi submethod encode_schema(Avro::Record $schema, IO::Handle $handle, Associative:D $hash) { 
    #   X::Avro::EncodeFail.new(:schema($schema),:data($hash)).throw() 
    #    unless $schema.is_valid_default($hash);
      for $schema.fields.list -> $field {
        my $data = $hash{$field.name};
        self.encode_schema($field.type,$handle,$data);
      }
    }   

    # encode an array in blocks of max size $!blocksize
    multi submethod encode_schema(Avro::Array $schema, IO::Handle $handle, Positional:D $arr) { 
      my @copy = $arr.clone();
      my Int $iterations = ($arr.elems() div $!blocksize);
      my Int $leftover = $arr.elems() mod $!blocksize;
      my @blocks = (1..$iterations).map:{ $!blocksize }; 
      push(@blocks, $leftover) if ($leftover > 0);
      for @blocks -> $size {
        $handle.write(encode_long($size));
        for 1..$size {
          self.encode_schema($schema.items,$handle,@copy.shift);
        }
      }
      $handle.write(encode_long(0));
    }   

    # todo set maps with negative counts ?
    multi submethod encode_schema(Avro::Map $schema, IO::Handle $handle, Associative:D $hash) { 
      my Avro::Schema $keyschema = Avro::String.new();
      my @kv = $hash.kv;
      my Int $iterations = ($hash.elems() div $!blocksize);
      my Int $leftover = $hash.elems() mod $!blocksize;
      my @blocks = (1..$iterations).map:{ $!blocksize }; 
      push(@blocks, $leftover) if ($leftover > 0);
      for @blocks -> $size {
        $handle.write(encode_long($size));
        for (1..$size) -> $i {
          self.encode_schema($keyschema,$handle,@kv.shift);  
          self.encode_schema($schema.values,$handle,@kv.shift);
        }
      }
      $handle.write(encode_long(0));
    } 

    multi submethod encode_schema(Avro::Enum $schema, IO::Handle $handle, Str:D $str) { 
      my Int $result = $schema.sym.first-index: { ($^a eq $str) }; 
      if $result.defined {
        $handle.write(encode_long($result));
      } else { X::Avro::EncodeFail.new(:schema($schema),:data($str)).throw()  }
    } 

    multi submethod encode_schema(Avro::Union $schema, IO::Handle $handle, Mu $data) { 
      my Avro::Schema $type = $schema.find_type($data);
      my Int $index = $schema.types.first-index: { ($^a ~~ $type) };
      $handle.write(encode_long($index));
      self.encode_schema($type,$handle,$data);
    }

    multi submethod encode_schema(Avro::Fixed $schema, IO::Handle $handle, Str:D $str) { 
      X::Avro::EncodeFail.new(:schema($schema),:data($str)).throw() 
        unless $schema.size == $str.codes(); 
      $handle.write(pack(template($schema.size),$str.ords()));
    }

    multi submethod encode_schema(Avro::Null $schema, IO::Handle $handle, Any:U $any) { 
       # $handle.write(pack("C",0));  --> misinterpretation
    }

    multi submethod encode_schema(Avro::String $schema, IO::Handle $handle, Str:D $str) { 
      my Blob $encoding = $str.encode();
      $handle.write(encode_long($encoding.elems())); 
      $handle.write($encoding);
    }

    multi submethod encode_schema(Avro::Bytes $schema, IO::Handle $handle, Str:D $str) { 
      $handle.write(encode_long($str.codes())); 
      $handle.write(pack(template($str.codes()),$str.ords()))
    }

    multi submethod encode_schema(Avro::Boolean $schema, IO::Handle $handle, Bool:D $bool) {  
      $handle.write(pack("C",$bool));
    }   

    multi submethod encode_schema(Avro::Integer $schema, IO::Handle $handle, Int:D $int) {
      $handle.write(encode_long($int)); 
    }

    multi submethod encode_schema(Avro::Long $schema, IO::Handle $handle, Int:D $long) {  
      $handle.write(encode_long($long)); 
    }

    multi submethod encode_schema(Avro::Float $schema, IO::Handle $handle, Rat:D $float) { 
      my @arr = int_to_bytes(to_floatbits($float),4);
      $handle.write(pack(template(4),@arr));
    }

    multi submethod encode_schema(Avro::Double $schema, IO::Handle $handle, Rat:D $double) {  
      my @arr = int_to_bytes(to_doublebits($double),8); 
      $handle.write(pack(template(8),@arr));
    }

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

