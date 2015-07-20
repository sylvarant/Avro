use v6;
use JSON::Tiny;
use Avro::Auxiliary;
use Avro::Schema;

package Avro{

  #======================================
  # Exceptions
  #======================================

  class X::Avro::DecodeFail is Avro::AvroException {
    has Avro::Schema $.schema;
    method message { "Failed to decode "~$!schema.type() }
  }


  #== Role ==============================
  #   * Decoder
  #======================================

  role Decoder {
    method decode(Avro::Schema, Blob:D --> Mu:D) { * };
  }


  #== Class =============================
  #   * BinaryDecoder
  #======================================

  class BinaryDecoder does Decoder { 

    # integers and longs are encode as variable sized zigzag numbers
    sub decode_long(BlobStream $stream){
      my @arr = ();
      my int $byte;
      repeat {
        $byte = $stream.read(1).unpack("C");
        push(@arr,$byte);
      } until (($byte +> 7) == 0); 
      return from_zigzag(from_varint(@arr));
    }

    multi submethod decode_schema(Avro::Record $schema,BlobStream $stream) { 
      my %hash;
      for $schema.fields.list -> $field {
        %hash{$field.name} = self.decode_schema($field.type,$stream);
      }
      return %hash;
    }   

    multi submethod decode_schema(Avro::Array $schema, BlobStream $stream) { 
      my Int $size = decode_long($stream);
      my @arr = ();
      while $size {
        for (1..$size) -> $i {
          push(@arr,self.decode_schema($schema.items,$stream));  
        }
        $size = decode_long($stream);
      } 
      return @arr
    }   

    multi submethod decode_schema(Avro::Map $schema, BlobStream $stream) { 
      my Int $size = decode_long($stream);
      my %hash;
      my Avro::Schema $keyschema = Avro::String.new();
      while $size {
        for (1..$size) -> $i {
          my Str $key = self.decode_schema($keyschema,$stream);
          my $data = self.decode_schema($schema.values,$stream);
          %hash{$key} = $data;
        }
        $size = decode_long($stream);
      }
      return %hash;
    }   

    multi submethod decode_schema(Avro::Enum $schema, BlobStream $stream) { 
      my int $result = decode_long($stream);
      $schema.sym[$result];
    }   

    multi submethod decode_schema(Avro::Union $schema, BlobStream $stream) { 
      my Int $num = decode_long($stream);
      my Avro::Schema $type = $schema.types[$num];
      self.decode_schema($type,$stream);
    }   

    multi submethod decode_schema(Avro::Fixed $schema, BlobStream $stream) { 
      my @arr = ();
      for (1..$schema.size) -> $i {
        push(@arr,$stream.read(1).unpack("C").chr);
      }
      @arr.join("");
    }   

    multi submethod decode_schema(Avro::Null $schema, BlobStream $stream) { 
      #my $r = $stream.read(1).unpack("C"); 
      #if $r == 0 { Any }
      #else { X::Avro::DecodeFail.new(:schema($schema)).throw()  }
      Any 
    }   

    multi submethod decode_schema(Avro::String $schema, BlobStream $stream) { 
      my int $size = decode_long($stream); 
      my Blob $r = $stream.read($size);
      $r.decode()
    }   

    multi submethod decode_schema(Avro::Bytes $schema, BlobStream $stream) { 
      my int $size = decode_long($stream); 
      my @arr = ();
      for 1..$size -> $i {
        push(@arr,$stream.read(1).unpack("C").chr);
      }
      @arr.join("");
    }   

    multi submethod decode_schema(Avro::Boolean $schema, BlobStream $stream) {  
      my $r = $stream.read(1).unpack("C"); 
      given $r {
        when 0  { False }

        when 1  { True }

        default { X::Avro::DecodeFail.new(:schema($schema)).throw()  }
      }
    }   

    multi submethod decode_schema(Avro::Integer $schema, BlobStream $stream) { 
      decode_long($stream);   
    }   

    multi submethod decode_schema(Avro::Long $schema, BlobStream $stream) { 
      decode_long($stream);
    }   

    multi submethod decode_schema(Avro::Float $schema, BlobStream $stream) { 
      my @arr = ();
      for 1..4 -> $i {
        push(@arr,$stream.read(1).unpack("C"));
      }
      from_floatbits(int_from_bytes(@arr));
    }   

    multi submethod decode_schema(Avro::Double $schema, BlobStream $stream) { 
      my @arr = ();
      for 1..8 -> $i {
        push(@arr,$stream.read(1).unpack("C"));
      }
      from_doublebits(int_from_bytes(@arr));
    }   

    method decode(Avro::Schema $schema, Blob $blob) {  
      # TODO check stream
      #X::Avro::DecodeFail.new(:note("End of file")).throw() if $stream.eof;
      {
        my BlobStream $stream = BlobStream.new(:blob($blob));
        return self.decode_schema($schema,$stream); 
        CATCH { default { X::Avro::DecodeFail.new(:schema($schema)).throw() }}
      }
    };

  };


  #== Class =============================
  #   * JSONDecoder
  #======================================

  class JSONDecoder does Decoder {
  
  
  };

}

