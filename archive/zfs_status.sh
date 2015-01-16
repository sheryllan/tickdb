zfs get compressratio
#You can only get dedup ratio for the entire pool
# (any volumes with dedup off will just be skipped, but still count in ratio)
# As such I enabled dedup for the entire storage volume
zpool list storage
perl -e 'print "="x80; print "\n";'
zpool iostat
perl -e 'print "-"x80; print "\n";'
zpool status
