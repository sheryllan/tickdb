#!/usr/bin/perl
# This script iterates through devices and sees which have FUJITSU ETERNUS in the name
# Very good to find all the drives that will be assembled into the pools/volumes and arrays

my @devices=`ls /dev/da*[0-9]`;

print "Checking for FUJITSU ETERNUS DXL SCSI drives..\n";
print "Devices: ";

foreach $device (@devices) {
	$device =~ s/\n//g;

	$inquiry = `camcontrol inquiry $device 2>&1 | head -n 1`;

	if ($inquiry =~ m/FUJITSU.ETERNUS_DXL.\d{4}/g) {
		print "$device ";
	}

}
print "\n";
