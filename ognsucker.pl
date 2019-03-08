#!/usr/bin/perl -w 

use strict;
use Ham::APRS::IS;
use Ham::APRS::FAP qw(parseaprs);
use Data::Dumper;
use POSIX qw(tzset);
use POSIX qw(strftime);
use Try::Tiny;

use DBI;
use DBD::mysql;

my $database = "ogn_data";
my $db = ConnectToMySql($database);

$ENV{TZ} = 'Pacific/Auckland';
tzset;

my $create_table_string = <<"TABLE_STRING";
(
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `thetime` datetime NOT NULL,
  `alt` int(11) DEFAULT NULL,
  `loc` point NOT NULL,
  `hex` char(6) NOT NULL DEFAULT '000000',
  `speed` smallint(6) DEFAULT NULL,
  `course` smallint(6) DEFAULT NULL,
  `type` tinyint(4) DEFAULT NULL,
  `rego` char(3) DEFAULT NULL,
  `vspeed` decimal(6,2) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `dateindex` (`thetime`),
  KEY `hexindex2` (`hex`,`thetime`),
  SPATIAL KEY `loc` (`loc`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
TABLE_STRING

#initiate the table name
my $table_name = strftime "data%Y%m%d", localtime;

print "connecting to server\n";

# England - lots of data during the day... a/54.69/-3.19/50.83/0.61
# Europe a/54.69/-3.19/44.69/14.86
# NZ a/-34.08/165.95/-48.51/181.29   a/-32.44/166.90/-47.51/184.29
# North Island circle r/-38.332,175.84/500
# omarama r/-44.477/+169.974/201.4
my $is = new Ham::APRS::IS('aprs.glidernet.org:14580', 'GNZSlurp', 'appid' => 'GNZ Slurp', 'filter'=>'r/-38.332/175.84/1200');
$is->connect('retryuntil' => 3) || die "Failed to connect: $is->{error}";
 
my $lastkeepalive = time();

# set DB timezone correctlhy
$db->do("SET time_zone = '+00:00';");

while($is->connected()) {
 
    # make sure we send a keep alive every 240 seconds or so                                               
    my $now = time();
    if( $now - $lastkeepalive > 240 ) {
        $is->sendline('# example code');
        $lastkeepalive = $now;
    }
 
    # read the line from the server
    my $line = $is->getline();
    next if (!defined $line);
 
    # parse the aprs packet
    my %packetdata;
    my $retval = parseaprs($line, \%packetdata);
 
    # and display it on the screen
    my $hex = "";

    if ($retval == 1) {
        #print Dumper( \%packetdata );
        if ($packetdata{"type"} eq "location") {
            if(exists($packetdata{"comment"})) {
              if (substr($packetdata{"comment"}, 0, 2) eq "id") {
                  $hex = substr($packetdata{"srccallsign"}, 3);
                  my $table_name = strftime "data%Y%m%d", localtime;

                  try {
                      $db->do("INSERT INTO ". $table_name ." (thetime, alt, loc, hex, type, speed, course) VALUES (FROM_UNIXTIME(" . $packetdata{"timestamp"} . "), " . $packetdata{"altitude"} . ", POINT(".$packetdata{"latitude"}.",".$packetdata{"longitude"}."1), '".$hex."', 1, ". $packetdata{"speed"} .", ". $packetdata{"course"} .");");
                  } catch {
                      #if we get an error, todays table might not exist
                      my $sql_date = strftime "%Y-%m-%d", localtime;
                      $db->do("CREATE TABLE IF NOT EXISTS ".$table_name. " " . $create_table_string);

                      # insert into days table
                      $db->do("INSERT INTO days SET day_date='" . $sql_date . "';");

                      # try inserting again
                      $db->do("INSERT INTO ". $table_name ." (thetime, alt, loc, hex, type, speed, course) VALUES (FROM_UNIXTIME(" . $packetdata{"timestamp"} . "), " . $packetdata{"altitude"} . ", POINT(".$packetdata{"latitude"}.",".$packetdata{"longitude"}."1), '".$hex."', 1, ". $packetdata{"speed"} .", ". $packetdata{"course"} .");");
                  }
              }
            }
        }
        #print ".\n";
    }
}
 
$is->disconnect() || die "Failed to disconnect: $is->{error}";




#--- start sub-routine ------------------------------------------------
sub ConnectToMySql {
#----------------------------------------------------------------------


my $dsn = "DBI:mysql:ogn_data;localhost";
my $dbh = DBI->connect($dsn, 'ogn', '65tq9by', { PrintError => 1, RaiseError => 1 });
return $dbh;

}

#--- end sub-routine --------------------------------------------------