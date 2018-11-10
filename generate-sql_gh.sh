#!/bin/bash
path1=/apps/xx/data-temp/*.csv
path2=/apps/xx/data/
path3=/apps/xx/sql-data/

# remove=`rsync --remove-source-files -azv apps@xx.xx.xx.xx:/export/home/apps/xx/*.csv /apps/xx/data-temp/`

for f in $path1
do
  echo "Processing $f file..."
   filename=$(basename "$f")
  echo $f
  echo $filename
   if [ "$f" = "$path1" ]; then
echo "empty"
else
	$name=$(echo "$filename" | cut -f 1 -d '.')
	echo "base name is $name"
	`python generate_sql_segment.py $f $path3$name.sql`

#    moving=`mv "$f" "$path2"`
fi
  # take action on each file. $f store current file name
done
