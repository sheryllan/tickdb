#!/bin/sh

influx -precision rfc3339 -format csv -database liquid_tick \
	-execute "select count(price) from trade group by time(1d),type,product,expiry" |\
sed 's/"//g' |\
grep -v '^name' |\
sed 's/T00:00:00Z//' |\
sed 's/^trade,//' |\
sed 's/expiry=//'|\
sed 's/product=//'|\
sed 's/type=//' |\
(echo 'expiry,product,type,date,count' && cat -) |\
awk -F ',' '{printf("%s,%s,%s,%s,%s\n",$3,$2,$1,$4,$5)}' |\
awk -F ',' '$NF>0' > trades.csv

influx -precision rfc3339 -format csv -database liquid_tick \
	-execute "select count(price) from book group by time(1d),type,product,expiry" |\
sed 's/"//g' |\
grep -v '^name' |\
sed 's/T00:00:00Z//' |\
sed 's/^book,//' |\
sed 's/expiry=//'|\
sed 's/product=//'|\
sed 's/type=//' |\
(echo 'expiry,product,type,date,count' && cat -) |\
awk -F ',' '{printf("%s,%s,%s,%s,%s\n",$3,$2,$1,$4,$5)}' |\
awk -F ',' '$NF>0' > books.csv
