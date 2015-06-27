#!/usr/bin/perl
#
# The script converts a binary-encoded log file into Splunk-friendly
# text.  It expects to receive the binary log file via STDIN
#
# Refer to the ___ manual for details on fields, formats, etc.
# 2011jul18    Created by David Millis, Splunk, Inc.

$Verbose = 0;
#$Verbose = 1;

#
# Lookup tables for enumerated values
#
%CDR_type_lookup = (
'0', 'IWFQNC',
'1', 'PDSN_BILL',
'2', 'ROAM',
);

use open ':encoding(utf8)';

#
# Interpret any command line args as a call for help
#
if ($ARGV[0]) {
  print STDOUT "cdr2text.pl usage: 'cat  | cdr2text.pl'\n";
  exit 0;
}

# Open STDIN
open(FILE, "-") or die "cannot open STDIN: $!";

binmode(FILE);

$recordcount=0;      # Keep track of the number of records processed
$derailcount=0;      # Check to see if we get "off" on start of record...

###############
# Step 1 - Pull in the raw data, 1 record at a time.
# Assign the values to a 'raw' hash
# Assume each record is 17 bytes long...
###############
while (read(FILE, $buff, 17)) {
  %raw = ();        # Hash for first-pass extractions
  %clean = ();      # Hash for cleaned up values, for output

  #
  # Use 'unpack' to extract all fields.  Some fields will need
  # further processing, which will be handled later.
  # Prepend fieldnames with the CDR field number
  #

  (
  $raw{'01a_Serial_Number'},
  $raw{'01b_Serial_Number'},
  $raw{'01c_Serial_Number'},
  $raw{'01d_Serial_Number'},
  $raw{'02_CDR_type'},
  $raw{'03a_Charge_start_time_year1'},
  $raw{'03b_Charge_start_time_year2'},
  $raw{'03c_Charge_start_time_month'},
  $raw{'03d_Charge_start_time_day'},
  $raw{'03e_Charge_start_time_hour'},
  $raw{'03f_Charge_start_time_minute'},
  $raw{'03g_Charge_start_time_second'},
  $raw{'04_Caller_party_number'}
  ) = unpack('
  H2 H2 H2 H2           # 01_Serial_Number
  H2                    # 02_CDR_type
  H2 H2 H2 H2 H2 H2 H2  # 03_Charge_start_time
  H10                   # 04_Caller_party_number
  h*', $buff);

  # Use the highest order byte of the Serial Number to determine
  # if we get "off" on processing records
  if (exists $raw{'01d_Serial_Number'} and $formerSN) {
    if ($raw{'01d_Serial_Number'} != $formerSN) {
      $derailcount++;
    }
  }
  $formerSN = $raw{'01d_Serial_Number'};

  ###############
  # Step 2 - Clean up fields, make them readable.
  # Put the translated values into the 'clean' hash
  ###############

  # 01_Serial_Number
  $clean{'01_Serial_Number'} = unpack("N",
                               pack("H2 H2 H2 H2",
                                    $raw{'01d_Serial_Number'},
                                    $raw{'01c_Serial_Number'},
                                    $raw{'01b_Serial_Number'},
                                    $raw{'01a_Serial_Number'}));

  # 02_CDR_type
  $tmp = unpack("I", pack("H8", $raw{'02_CDR_type'}));
  if (exists $CDR_type_lookup{$tmp}) {
    $clean{'02_CDR_type'} = $CDR_type_lookup{$tmp};
  } else {
    $clean{'02_CDR_type'} = "UNDEFINED";
  }

  # 03_Charge_start_time
  # output format: dd/mm/yyyy hh:mm:ss
  $tmp1 = unpack("N",
          pack("H2 H2 H2 H2",
               0x00,
               0x00,
               $raw{'03b_Charge_start_time_year2'},
               $raw{'03a_Charge_start_time_year1'}));
  $tmp2 = unpack("I",
          pack("H8", $raw{'03c_Charge_start_time_month'}));
  if ($tmp2 == 255) {
    $clean{'03_Charge_start_time'} = "-";
  } else {
    $tmp3 = unpack("I", pack("H8", $raw{'03d_Charge_start_time_day'}));
    $tmp4 = unpack("I", pack("H8", $raw{'03e_Charge_start_time_hour'}));
    $tmp5 = unpack("I", pack("H8", $raw{'03f_Charge_start_time_minute'}));
    $tmp6 = unpack("I", pack("H8", $raw{'03g_Charge_start_time_second'}));
    $clean{'03_Charge_start_time'} = sprintf("%4d/%02d/%02d %02d:%02d:%02d",
                                             $tmp1,
                                             $tmp2,
                                             $tmp3,
                                             $tmp4,
                                             $tmp5,
                                             $tmp6);
  }

  # 04_Caller_party_number
  $clean{'04_Caller_party_number'} = unpack("H10",
                                     pack("H10", $raw{'04_Caller_party_number'}));

  ###############
  # Step 3 - Output the translated values
  ###############

  #
  # Create a timestamp
  # Since each record does not have a true timestamp, use
  # 03_Charge_start_time
  if ($clean{'03_Charge_start_time'} != '-') {
    $Timestamp = $clean{'03_Charge_start_time'};
  } else {
    $Timestamp = "Unknown Timestamp:";
  }

  #
  # Output the field/value pairs in a consistent, splunk-ready fashion
  #
  print STDOUT "${Timestamp} ";
  foreach $name (sort (keys %clean)) {
    $fieldname = $name;
    $fieldname =~ s/[0-9a-z]+_//;    # Strip off the field number
    print STDOUT "${fieldname}=\"$clean{$name}\",";
  }
  print STDOUT "\n";
  if ($Verbose) {
    foreach $name (sort (keys %raw)) {
      print STDOUT "${name}=$raw{$name},";
      print STDOUT "\n";
    }
    print STDOUT "\n";
    print STDOUT "recordcount is $recordcount\n";
  }
  $recordcount++;
}

