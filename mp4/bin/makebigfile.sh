#! /bin/bash
rm -f bigfile
rm -f bigfile2
cat rfc2616.txt >> bigfile
for i in {1..4}
do
	cat bigfile >> bigfile2
	cat bigfile >> bigfile2
	cat bigfile2 >> bigfile
	cat bigfile2 >> bigfile
done
rm bigfile2
