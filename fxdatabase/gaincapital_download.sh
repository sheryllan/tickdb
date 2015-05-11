#!/bin/sh

cd /mnt/data/database/fx

parallel -I {} wget -rcN --level 5 'http://ratedata.gaincapital.com/20{}/' ::: {00..15}

if [ $? -eq 0 ]; then
	x="[GainCapital] download on `date` is complete"
	mail -s "$x" dbellot@liquidcapital.com < "."
else
	x="[GainCapital] download on `date` not finished"
	mail -s "$x" dbellot@liquidcapital.com < "."
fi
