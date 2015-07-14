use v6;
use JSON::Tiny;
use Avro::Auxiliary;

package Avro {


  #======================================
  # Exceptions
  #======================================

  class X::Avro::Type is Avro::AvroException {
    has $.type;
    method message { "Input $!type is not a valid Avro type" }
  }

  class X::Avro::Order is Avro::AvroException {
    has $.type;
    method message { "Input $!type is not a valid Order type" }
  }

  class X::Avro::Primitive is Avro::AvroException {
    has Str $.source;
    method message { "Input $!source is not a valid primitive type" }
  }

  class X::Avro::MissingName is Avro::AvroException {
    method message { "This Avro Schema requires a name" }
  }

  class X::Avro::Record is Avro::AvroException {
    has Str $.note;
    method message { "Not a valid Record Schema, $!note" }
  }

  class X::Avro::Union is Avro::AvroException {
    has Str $.note;
    method message { "Not a valid Union Schema, $!note" }
  }

  class X::Avro::Map is Avro::AvroException {
    has Str $.note;
    method message { "Not a valid Map Schema, $!note" }
  }

  class X::Avro::Array is Avro::AvroException {
    has Str $.note;
    method message { "Not a valid Map Schema, $!note" }
  }

  class X::Avro::Enum is Avro::AvroException {
    has Str $.note;
    method message { "Not a valid Enum Schema, $!note" }
  }

  class X::Avro::Fixed is Avro::AvroException {
    has Str $.note;
    method message { "Not a valid Fixed Schema, $!note" }
  }

  class X::Avro::Field is Avro::AvroException {
    has Str $.note;
    method message { "Not a valid Field Schema, $!note" }
  }


  #== Role ==============================
  #   * Avro Schema
  #======================================

  role Schema {
    has Iterable $!native;
    method native(--> Iterable) { return $!native; } 
    method is_valid_data($data) { ... } 
    method to_json(--> Str) { to-json(self.native()); }
    method is_valid_default(Mu:D --> Bool) { ... }
  }

  # parse produces Schema
  proto parse(Mu  --> Avro::Schema) is export {*}


  #== Role ==============================
  #   * Documented
  #   -- used by Enum and Record
  #======================================

  role Documented {
    has Str $.documentation = "";
  }


  #== Role ==============================
  #   * Aliased
  #   -- used by NamedComplex & Field
  #======================================

  role Aliased {
    has Positional $.aliases = ();
  }


  #== Enum ==============================
  #   * Order
  #   -- used by Field 
  #======================================

  enum Order <ascending descending ignore>;

  sub parse_order(Str $text){
    given $text {

      when "ascending" { Order::ascending }

      when "descending" { Order::descending }

      when "ignore" { Order::ignore }

      default { X::Avro::Order.new(:type($text)).throw() }
    }
  }

  sub order_str (Order $o){
    given $o {

      when Order::ascending { "ascending" }

      when Order::descending { "descending" }

      when Order::ignore { "ignore" }
    }
  }


  #== Class =============================
  #   * NamedComplex 
  #   -- used by Enum,Record,Fixed 
  #======================================

  class NamedComplex does Aliased {
    
    has Str $.name;
    has Str $.namespace;
    has Str $.fullname;

    submethod BUILD(Associative :$hash){
      X::Avro::MissingName.new().throw() unless $hash{'name'}:exists;
      $!namespace = ""; 
      $!name = $hash{'name'};
      $!fullname = $!name;

      # namespace
      if $hash{'namespace'}:exists {
        $!namespace = $hash{'namespace'}; 
        $!fullname = $!namespace ~ "." ~ $!fullname;
      }

      # aliases
      if $hash{'aliases'}:exists {
        # TODO
      }
    }
  }


  #== Class =============================
  #   * Field 
  #   -- A member of Records
  #======================================

  class Field does Aliased {

    also does Documented;
     
    has EnumMap $.native;
    has Str $.name;
    has Avro::Schema $.type;
    has $.default;
    has Order $.order;

    submethod BUILD(Associative :$hash){
      X::Avro::Field.new(:note("Missing type and/or name")).throw() 
        unless $hash{'name'} and $hash{'type'}; 
      $!name = $hash{'name'};
      $!type = parse($hash{'type'});
      $!order = Order::ascending;
      $!order = parse_order($hash{'order'}) if $hash{'order'}:exists; 
      if $hash{'doc'}:exists {
        $!documentation = $hash{'doc'}; 
        $!native{'doc'} = $!documentation;
      }
      if $hash{'default'}:exists {
          $!default = $hash{'default'};
          X::Avro::Field.new(:note("Invalid default value: "~ ($hash{'default'}))).throw()  
            unless $!type.is_valid_default($!default); 
          $!native{'default'} = $!default;
      }
      my Str $o = order_str($!order);
      $!native = { 'name' => $!name, 'type' => $!type.native() , 'order' => $o}; 
      $!native{'aliases'} = self.aliases() if self.aliases.defined;
    }
  }


  #== Class =============================
  #   * Primitive Type
  #======================================

  class Primitive does Schema {

    constant @primitive_types = <null boolean int long float double bytes string>; 
    has Str $.type;

    sub is_primitive (Str $type --> Bool){
      my $result = first-index { ($^a eq $type) }, @primitive_types; 
      return $result.defined; 
    }
    
    submethod BUILD(:$type){
      $!type = $type;
      X::Avro::Primitive.new(:source($type)).throw() unless is_primitive($type);
      $!native = EnumMap.new("type",$type); 
    }

    method native(--> EnumMap){
      return $!native;
    }

    method is_valid_data ($data){
       return True; 
    }

    method is_valid_default(Cool:D $default){
      given $!type {
        when "string"   { $default ~~ Str }
        when "boolean"  { $default ~~ Bool }
        when "null"     { $default ~~ Any }
        when "bytes"    { $default ~~ Str } #Todo unsigned
        when "int" | "long"   { $default ~~ Int } 
        when "float" | "double" { $default ~~ Rat }
      }
    }
  }


  #== Class =============================
  #   * Array Type
  #======================================

  class Array does Schema {
    
    has Avro::Schema $.items;

    constant type = 'array';
    
    submethod BUILD(:$hash){
      X::Avro::Array.new(:note("Requires values")).throw() unless $hash{'items'}:exists;
      $!items = parse($hash{'items'});
      my Iterable $other = $!items.native();
      $!native = EnumMap.new("type",type,"items",$other);
    }
    
    method native(--> EnumMap){
      return $!native;
    }

    method is_valid_data (Positional:D $data){
      for $data.values -> $item {
        return False unless $!items.is_valid_data($item);
      }
    }

    method is_valid_default(Positional:D $array){ 
      return self.is_valid_data($array);
    }

  }


  #== Class =============================
  #   * Map Type
  #======================================

  class Map does Schema {

    has Avro::Schema $.values;

    constant type = 'map';

    submethod BUILD(Associative :$hash){
      X::Avro::Schema.new(:note("Requires values")).throw() unless $hash{'values'}:exists;
      $!values = parse($hash{'values'});
      my Iterable $other = $!values.native();
      $!native = EnumMap.new("type",type,"values",$other);
    }

    method native(--> EnumMap){
      return $!native;
    }

    method is_valid_data (Associative:D $data){
      for $data.values -> $item {
        return False unless $!values.is_valid_data($item);
      }
    }

    method is_valid_default(Associative:D $hash) { 
      return self.is_valid_data($hash);
    }

  }


  #== Class =============================
  #   * Union Type
  #======================================

  class Union does Schema {

    has List $.types;

    submethod BUILD(Positional :$types){
      $!types = $types.map: { parse($_) };
      my %encountered;
      for $!types.values -> $schema {
        X::Avro::Union.new("Union not permitted") if $schema ~~ Avro::Union;
        if $schema ~~ Avro::NamedComplex {
          my Str $resolved = $schema.WHAT.gist ~ $schema.fullname(); #TODO resolve aliases
          X::Avro::Union.new(:note("Duplicate Complex type of name: "~$schema.name())).throw()  if %encountered{$resolved}:exists;
          %encountered{$resolved} = 1;
        } else {
          my Str $key = $schema.WHAT.gist ~ ($schema.?type().gist);
          X::Avro::Union.new(:note("Duplicate Primitive type: "~$key)).throw()  if %encountered{$key}:exists;
          %encountered{$key} = 1;
        }
      }
      $!native = $!types.map:{ $_.native() };
    }

    method native(--> List){
      return $!native;
    }

    method is_valid_data (Mu:D $data){
      for $!types.values -> $type {
        return True if $type.is_valid_default($data);
      }
      return False;
    }

    method is_valid_default(Mu:D $value){
      $!types[0].is_valid_default($value);
    }

  }


  my $break_lazy = 0;

  #== Class =============================
  #   * Record Type
  #======================================

  class Record is NamedComplex does Schema  {
    
    also does Documented;

    constant type = "record";
    has List $!fields;

    sub create_field(Associative:D $hash --> Avro::Field) { Avro::Field.new(:hash($hash)) }

    submethod BUILD(Associative:D :$hash){
      X::Avro::Record.new(:note("Missing Fields!")).throw() 
        unless $hash{'fields'}:exists;
      my List $ls = $hash{'fields'}.values;
      $!fields = $ls.map: { create_field($_) }; 
      $break_lazy = $!fields.elems(); #force the computations --> this perl6 lazy map is obnoxius
      my $nativesf = $!fields.map:{ $_.native() };
      $!native = { 'type' => type, 'name' => self.name(), 'fields' => $nativesf.values };
      $!native{'aliases'} = self.aliases() unless self.aliases() ~~ ();
      $!native{'namespace'} = self.namespace() unless self.namespace() eq "";
      if $hash{'doc'}:exists {
        $!documentation = $hash{'doc'}; 
        $!native{'doc'} = $!documentation;
      }
    }

    method is_valid_data ($data){
      return True;
    }
    
    method is_valid_default(Associative:D $hash){
      return self.is_valid_data($hash);
    }
     
  }


  #== Class =============================
  #   * Enum Type
  #======================================

  class Enum is NamedComplex does Schema {

    also does Documented;

    constant type = "enum";

    has List $!sym;
    
    submethod BUILD(Associative :$hash) {
      X::Avro::Enum.new(:note("Requires symbols")).throw() unless $hash{'symbols'}:exists;
      $!sym = $hash{'symbols'};
      if $hash{'doc'}:exists {
        $!documentation = $hash{'doc'};
        $!native{'doc'} = $!documentation;
      }
      $!native{ 'type' } = type; 
      $!native{'name'} = self.name(); 
      $!native{'aliases'} = self.aliases() unless self.aliases() ~~ ();
      $!native{'namespace'} = self.namespace() unless self.namespace() eq "";
      $!native{'symbols'} = $!sym;
    }

    method is_valid_data ($data){
      my $result = $!sym.first-index: { ($^a eq $data) }; 
      return $result.defined; 
    }

    method is_valid_default(Str $str){
      return self.is_valid_data($str);
    }

  }


  #== Class =============================
  #   * Fixed Type
  #======================================

  class Fixed is NamedComplex does Schema {

    has Int $!size;

    constant type = "fixed";

    submethod BUILD(Associative :$hash){
      X::Avro::Fixed.new(:note("Requires size")).throw() unless $hash{'size'}:exists;
      $!size = $hash{'size'};
      $!native = { 'type' => type, 'name' => self.name(), 'size' => $!size};
      $!native{'aliases'} = self.aliases() unless self.aliases() ~~ ();
      $!native{'namespace'} = self.namespace() unless self.namespace() eq "";
    }

    method is_valid_data ($data){
      return True;
    }

    method is_valid_default(Str:D $str){
      return self.is_valid_data($str);
    }
  }


  #======================================
  # Schema parser 
  # -- Produces Schema Objects
  #======================================

  multi sub parse (Associative $hash --> Avro::Schema) is export {

    my Str $ty = $hash{'type'};
    return Avro::Primitive.new(:type($ty))if $hash.pairs == 1;

    given $ty {
    
      when 'record' { return Avro::Record.new(:hash($hash)); }

      when 'enum' { return Avro::Enum.new(:hash($hash)); }

      when 'fixed' { return Avro::Fixed.new(:hash($hash)); }

      when 'map' { return Avro::Map.new(:hash($hash)); }

      when 'array' { return Avro::Array.new(:hash($hash)); }

      default { X::Avro::Type.new(:source($ty)).throw(); }
    }
  }
  
  multi sub parse(Positional $arr --> Avro::Schema) is export {
    return Avro::Union.new(:types($arr)); 
  }

  multi sub parse(Str $str --> Avro::Schema) is export {
    #TODO name recognition
    return Avro::Primitive.new(:type($str));
  }

}

