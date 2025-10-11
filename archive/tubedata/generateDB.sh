#!/bin/bash
DBNAME=halfway.db
rm -f ./$DBNAME
for CSVS in *.csv
do
	python ./csv2sqlite.py $CSVS $DBNAME ${CSVS%.*}
done

