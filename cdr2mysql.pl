#!/usr/bin/perl
use Time::HiRes qw(gettimeofday);
use strict;
use Time::localtime;
use Digest::SHA1;
use Devel::Size qw(size total_size);
use DBI;

my $time1 = gettimeofday;

# GLOBAL VARIABLES
my $mid=1; # mid in billing
my $dir = "/BILLING/ATS/cdr/"; # main directory
my $tm = localtime;

print("###------------------------------------------###\n");
print("TIME: ".$tm->mday.".".($tm->mon+1).".".($tm->year+1900)." ".$tm->hour.":".$tm->min."\n");
#my $cdr = sprintf("%scdr_log_mon%02d_%04d.log",$dir,($tm->mon+1),($tm->year+1900));
my %DATA = (); # data hash

# convert to format Е.164
sub num2e164
{
   my $num = shift;
   chomp($num);
#      if($num ne '-'){
        # если 10 цифр - приписываем 7 ку
        if( $num =~ s/^(\d{10})$/7$1/ )
        {
        }
        # если от 1 до 7 - приписываем код города
        elsif( $num =~ s/^(\d{7})$/7812$1/ )
        {
        }
        # отбрасываем 810
        elsif( $num =~ s/^810(\d+)$/$1/ )
        {
        }
        # отбрасываем 8
        elsif( $num =~ s/^8(\d+)$/7$1/ )
        {
        }
        return $num;
#      }else{
#	return "err";
#      }
}

# READ CDR FILES ARGV1=$CDR_FILE_NAME
sub read_cdr_file
{
	my $cdr = shift;

	my $dsn = 'DBI:mysql:bill:localhost';
	my $db_user_name = 'bill';
	my $db_password = 'billpass';
	my $dbh = DBI->connect($dsn, $db_user_name, $db_password,{ RaiseError => 1 });
	my $insert_handle = $dbh->prepare_cached("INSERT IGNORE INTO cdr VALUES (?,?,?,?,?,?)");

	print "Open file $cdr\n";
	open(CDR, $cdr) || die "File not found\n";
	while(<CDR>)
	{
#				30     .08      .2012     14     :29     :44	    107	   -	      -	         1025	    -	       A=1025	 A1025	0	107
			if ( /^(\d{2})\.(\d{2})\.(\d{4})\s(\d{2}):(\d{2}):(\d{2})\s(\d+)\s([\w\-]+)\s([\w\-]+)\s([\w\-]+)\s([\w\-]+)\s([\w\-\=]+)\s(\w+)\s(\w+)\s(\d+)/ )
			{

					my ($d,$m,$y,$hh,$mm,$ss,$long,$from,$from_e164,$to,$to_e164,$from_p,$to_p,$cat,$long2) = ($1,$2,$3,$4,$5,$6,$7,($8),($9),($10),($11),$12,$13,$14,$15);

					my ($r_from,$r_to) = "";

					if($from_e164 ne '-'){
					   $r_from = num2e164($from_e164);
					}else{
					   if($from ne '-'){
						$r_from = num2e164($from);
					   }else{
						$r_from = "-";
					   }
					}

					if($to_e164 ne '-'){
					   $r_to = num2e164($to_e164);
					}else{
					   if($to ne '-'){
						$r_to = num2e164($to);
                                           }else{
						$r_to = "-";
                                           }
					}

					#my $str = sprintf("%02d.%02d.%04d %02d:%02d:%02d\t%s\t%s\t%s\t%s\t%s\t%s\t%s",$d,$m,$y,$h,$mm,$ss,$long,$r_from,$r_to,$from_p,$to_p,$cat,$long2);
					my $timestr = sprintf("%02d-%02d-%04d %02d:%02d:%02d",$y,$m,$d,$hh,$mm,$ss);
					#print("long: $long\n");
					$insert_handle->execute($timestr,$r_from,$r_to,$from_p,$to_p,$long) || die "Can not write mysql";

			}else{
					print "-ERROR-($_)\n";
			}
	}
	close(CDR);

	$dbh->disconnect();
}

# GET CDR FILE LIST
sub get_cdr_list
{
	opendir(DIR, $dir) || die "Can't open dir $dir\n";
	my @list = grep {/\.log/} readdir(DIR);
	foreach(@list)
	{
		if( $_ ne "." && $_ ne ".." )
		{
			my $file_name = $dir.$_;
			chomp($file_name);
			print "Processing file $file_name\n";
			read_cdr_file($file_name);
		}		
	}
	
	closedir(DIR);	
}

# MAIN FUNCTION
&get_cdr_list();

my $time2 = gettimeofday;
my $timebenchmark = $time2 - $time1;
print("worktime: ".($timebenchmark)." sec\n");
print("DATA total_size:". total_size(\%DATA) ."\n");
print("###------------------------------------------###\n");
