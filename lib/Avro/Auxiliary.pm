use v6;
use JSON::Tiny;

package Avro {

  #======================================
  # Exceptions
  #======================================

  class AvroException is Exception {
    method message { "Something went wrong" }
  }

  #======================================
  # Low-level representations
  #======================================

  sub to_zigzag(int $n) is export {
    ($n < 0) ?? ((($n +< 1) +^ (-1)) +| 1) !! ($n +< 1)
  }

  sub from_zigzag(int $n) is export {
    ($n +& 1) ?? (-(1 + ($n +> 1))) !! ($n +> 1)
  }

  sub to_varint(int $n --> Positional:D) is export {
    my $iter = $n;
    my @result = ();
    while ( ($iter +> 7) > 0) {
      my int $byte = ($iter +& 127) +| 128;
      push(@result,$byte);
      $iter = $iter +> 7;
    }
    push(@result,$iter);
    return @result;
  }

  sub from_varint(Positional $arr --> int) is export {
    my $r = 0;
    my $position = 0;
    for $arr.values -> $byte {
      $r = $r +| (($byte +& 127) +< $position);
      $position += 7; 
    }
    return $r;
  }

}
