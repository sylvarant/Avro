=begin pod
=head1 JSON::Tiny
C<JSON::Tiny> is a minimalistic module that reads and writes JSON.
It supports strings, numbers, arrays and hashes (no custom objects).
=head1 Synopsis
    use JSON::Tiny;
    my $json = to-json([1, 2, "a third item"]);
    my $copy-of-original-data-structure = from-json($json);
=end pod

use v6;
use JSON::Tiny;
use Avro::Schema;
use Avro::Auxiliary;


module Avro:ver<0.01> {

  #======================================
  # Schema parser interface
  #======================================

  proto parse-schema($) is export {*}

  multi sub parse-schema(Str $text) {
    my Avro::Schema $s = parse(from-json($text)); 
    CATCH {
      when X::JSON::Tiny::Invalid  {
        # For reasons beyond my comprehension 
        # Perl JSON doesn't accept JSON strings as input
       return parse($text); 
      }

      default { $_.throw();}
    }
    return $s;
  }

  multi sub parse-schema(Associative $hash) {
    return parse($hash);
  }

  multi sub parse-schema(Positional $array) {
    return parse($array);
  }

}

