use v6;
use Test;
use lib 'lib';
use Avro; 
use Avro::Auxiliary;

plan 10;

#======================================
# Test ZigZag
#======================================

is to_zigzag(1),2,"To zigzag 1 works";
is to_zigzag(-2),3,"To zigzag -2 works";
is from_zigzag(4294967294),2147483647,"From zigzag large number works";
is from_zigzag(4294967295),-2147483648,"From zigzag small number works";


#======================================
# Variable int
#======================================

my %hash =  128 => [128,1], 130 => [130,1], 16383 => [0xff,127];

for %hash.keys -> $num {
  is-deeply to_varint(+$num),%hash{$num},"To variable int works for $num";
  is-deeply from_varint(%hash{$num}),+$num,"From variable int works for $num";
}


