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

  sub int_to_bytes(int $n, int $count --> Positional:D) is export {
    my $iter = $n;
    my @bytes = ();
    for 1..$count -> $i {
      my int $byte = $iter +& 0xff; 
      push(@bytes,$byte);
      $iter = $iter +> 8;  
    }
    return @bytes;
  }

  sub int_from_bytes(Positional:D $arr --> int) is export {
    my $position = 0;
    my int $result = 0;
    for $arr.values -> $byte {
      $result = $result +| ($byte +< $position);
      $position += 8;
    }
    return $result;
  }


  #======================================
  # Floating point stuff - perl6 lacking
  #======================================

  sub frexp(Rat $rat --> Positional:D) is export {

    return (0,0) if $rat == 0; 
    return ((-1),NaN) if $rat == NaN;
    return (-1,$rat) if $rat == -Inf or $rat == Inf;

    my Int $exp = 0;
    my Rat $mantissa = $rat;
    my $sign = $mantissa.sign();

    if ($mantissa.sign() == -1) {
      $mantissa = -$mantissa; 
    }

    while ($mantissa < 0.5) {
      $mantissa *= 2.0;
      $exp -= 1;
    }

    while ($mantissa >= 1.0) {
      $mantissa *= 0.5;
      $exp++;
    }

    $mantissa = $mantissa * $sign;
    return ($exp,$mantissa);
  }

  # Java : floatToIntBits !temporary
  sub to_floatbits(Rat $rat --> int) is export {
    given $rat {
  
      when NaN {  0x7fc00000 }

      when Inf { 0x7f800000 }

      when -Inf { 0xff800000 }

      when 0 { 0 }

      default { 

        my Rat $neutr = $rat.sign() == -1 ?? -$rat !! $rat;
        my Int $nat = $neutr.truncate;

        # compute posible positive exp
        my $plus = 0;
        my $iter = $nat;
        until $iter <= 1  {
          $plus += 1;
          $iter = $iter +> 1;
        }

        # compute fraction bits
        my Rat $comma = $neutr - ($neutr.truncate).Rat;
        my Int $fraction = 0;
        my $mask = 1;
        my $bytes = 0;
        my $neg = 0;
        until $comma == 0.0 or (23 - $plus - $bytes) == 0  {
          $bytes++;
          $comma *= 2.0;
          if $comma.truncate > 0 {
           $neg = $bytes if $neg == 0; 
           $fraction = $fraction +| $mask;
          }
          $comma = $comma - ($comma.truncate).Rat;
          $mask = $mask +< 1;
        }
        my Int $final = 0;
        loop (my $i = 0; $i < $bytes; $i++) {
          $final = $final +| ( 1 +< ($bytes - ($i +1))) if $fraction +& 0x1;
          $fraction = $fraction +> 1;
        }
        my int $f = 0; 
        my $exp = $plus > 0 ?? $plus !! -$neg;
        $f = $f +| $final;
        $f = $f +| ( $nat +< $bytes);
        $f = ($f +< (23 - $bytes - $exp)) +& 0x007fffff; 
        $exp = ($exp + 127) +< 23;
        my $result = 0; 
        $result = $result +| 0x80000000 if $rat.sign() == -1;
        $result = $result +| $exp;
        $result = $result +| $f;
        return $result;
      }

    }
  }

  # read in bytes
  sub from_floatbits(int $n --> Rat:D) is export {

    given $n  {

      when 0x7fc00000 { NaN }

      when 0x7f800000 { Inf }

      when 0xff800000 { -Inf }

      when 0 { 0 }

      default { 
        my $sign = 1; 
        $sign = -1 if $n +& 0x80000000;
        my Int $exp = (($n +> 23) +& 0xff); 
        $exp -= 127;
        my Int $fract =  ($n +& 0x007fffff);
        my Rat $rat = 1.Rat;
        my $iter = $fract;
        for (0..22) -> $i {
         $rat += (2**($i - 23)) if $iter +& 0x1;
         $iter = $iter +> 1;
        }
        $rat = $sign.Rat * $rat * (2**$exp).Rat; 
      }
    }
  }

}
