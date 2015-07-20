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
    method encode(Avro::Schema,Mu:D --> List) { * };
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

    multi submethod encode_schema(Avro::Record $schema, Associative:D $hash) { 
    #   X::Avro::EncodeFail.new(:schema($schema),:data($hash)).throw() 
    #    unless $schema.is_valid_default($hash);
      my @ls;
      for $schema.fields.list -> $field {
        my $data = $hash{$field.name};
        push @ls, self.encode_schema($field.type,$data);
      }
      @ls
    }   

    # encode an array in blocks of max size $!blocksize
    multi submethod encode_schema(Avro::Array $schema, Positional:D $arr) { 
      my @ls;
      my @copy = $arr.clone();
      my Int $iterations = ($arr.elems() div $!blocksize);
      my Int $leftover = $arr.elems() mod $!blocksize;
      my @blocks = (1..$iterations).map:{ $!blocksize }; 
      push(@blocks, $leftover) if ($leftover > 0);
      for @blocks -> $size {
        push @ls, (encode_long($size));
        for 1..$size {
          push @ls, self.encode_schema($schema.items,@copy.shift);
        }
      }
      push @ls, encode_long(0);
      @ls
    }   

    # todo set maps with negative counts ?
    multi submethod encode_schema(Avro::Map $schema, Associative:D $hash) { 
      my @ls;
      my Avro::Schema $keyschema = Avro::String.new();
      my @kv = $hash.kv;
      my Int $iterations = ($hash.elems() div $!blocksize);
      my Int $leftover = $hash.elems() mod $!blocksize;
      my @blocks = (1..$iterations).map:{ $!blocksize }; 
      push(@blocks, $leftover) if ($leftover > 0);
      for @blocks -> $size {
        push @ls, (encode_long($size));
        for (1..$size) -> $i {
          push @ls, self.encode_schema($keyschema,@kv.shift);  
          push @ls, self.encode_schema($schema.values,@kv.shift);
        }
      }
      push @ls,(encode_long(0));
      @ls
    } 

    multi submethod encode_schema(Avro::Enum $schema, Str:D $str) { 
      my Int $result = $schema.sym.first-index: { ($^a eq $str) }; 
      if $result.defined {
        my @ls;
        return ( push @ls,(encode_long($result)) );
      } else { 
        X::Avro::EncodeFail.new(:schema($schema),:data($str)).throw() 
      }
    } 

    multi submethod encode_schema(Avro::Union $schema, Mu $data) { 
      my @ls;
      my Avro::Schema $type = $schema.find_type($data);
      my Int $index = $schema.types.first-index: { ($^a ~~ $type) };
      push @ls, (encode_long($index));
      push @ls, self.encode_schema($type,$data);
    }

    multi submethod encode_schema(Avro::Fixed $schema, Str:D $str) { 
      X::Avro::EncodeFail.new(:schema($schema),:data($str)).throw() 
        unless $schema.size == $str.codes(); 
      my @ls;
      push @ls, (pack(template($schema.size),$str.ords()))
    }

    multi submethod encode_schema(Avro::Null $schema, Any:U $any) { 
       # (pack("C",0));  --> misinterpretation
       ()
    }

    multi submethod encode_schema(Avro::String $schema, Str:D $str) { 
      my Blob $encoding = $str.encode();
      my @ls;
      push @ls, (encode_long($encoding.elems()));
      push @ls, $encoding;
    }

    multi submethod encode_schema(Avro::Bytes $schema, Str:D $str) { 
      ( encode_long($str.codes()) , pack(template($str.codes()),$str.ords()) )
    }

    multi submethod encode_schema(Avro::Boolean $schema, Bool:D $bool) {  
      my @ls;
      push @ls, (pack("C",$bool))
    }   

    multi submethod encode_schema(Avro::Integer $schema, Int:D $int) {
      my @ls;
      push @ls, encode_long($int)
    }

    multi submethod encode_schema(Avro::Long $schema, Int:D $long) {  
      my @ls;
      push @ls, encode_long($long); 
    }

    multi submethod encode_schema(Avro::Float $schema, Rat:D $float) { 
      my @ls;
      my @arr = int_to_bytes(to_floatbits($float),4);
      push @ls, (pack(template(4),@arr))
    }

    multi submethod encode_schema(Avro::Double $schema, Rat:D $double) {  
      my @ls;
      my @arr = int_to_bytes(to_doublebits($double),8); 
      push @ls, (pack(template(8),@arr))
    }

    method encode(Avro::Schema $schema, Mu $data) {  
      try {
      #  say $schema.WHAT.gist();
        return self.encode_schema($schema,$data); 
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

