#python3 -m cProfile -o prof.out database_builder.py -l 5 -o . -d qtg_mdp3 -f csv.bz2 -i /mnt/data/qtg/instRefdataCoh.csv -g log /mnt/data/qtg/BSXCBT3MA1/201511/cbot.151127.dat.gz
#kernprof -l -v database_builder.py -l 5 -o . -d qtg_mdp3 -f csv.bz2 -i /mnt/data/qtg/instRefdataCoh.csv -g log /mnt/data/qtg/BSXCBT3MA1/201511/cbot.151127.dat.gz
#./database_builder.py -l 5 -o . -d qtg_mdp3 -f csv.bz2 -i /mnt/data/qtg/instRefdataCoh.csv -g log /mnt/data/qtg/BSXCBT3MA1/201511/cbot.151127.dat.gz
./database_builder.py -l 5 -o . -d qtg_kospi -f csv.bz2 -i /mnt/data/qtg/instRefdataCoh.csv -g log /mnt/data/qtg/BSBUSKMA1/201511/kospi.151118.dat.gz
