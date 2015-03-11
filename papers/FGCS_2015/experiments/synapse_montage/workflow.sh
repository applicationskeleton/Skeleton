#!/bin/sh 

. ve/bin/activate

export SYNAPSE_DBURL=mongodb://localhost:27017/synapse_montage_01
export SYNAPSE_PROFILE=radical-synapse-profile.py

export PATH="`pwd`/../Montage_v3.3/bin:$PATH"

if test -f cleanme
then
    rm -rf corrdir
    rm -rf corrections.tbl
    rm -rf corr-images.tbl
    rm -rf diffdir
    rm -rf diffs.tbl
    rm -rf final
    rm -rf fits.tbl
    rm -rf images-rawdir.tbl
    rm -rf images.tbl
    rm -rf mBackground-list.txt
    rm -rf mDiffFit-list.txt
    rm -rf mProjectPP-list.txt
    rm -rf projdir
    rm -rf statdir
    rm -rf stats.tbl
    rm -rf template.hdr
    rm -rf *.log
    rm -rf cleanme
    echo 'all clean'
    exit
fi

mkdir projdir diffdir statdir corrdir final

$SYNAPSE_PROFILE mImgtbl  rawdir images-rawdir.tbl
$SYNAPSE_PROFILE mMakeHdr images-rawdir.tbl template.hdr

for file in rawdir/*
do 
    $SYNAPSE_PROFILE mProjectPP $file projdir/hdu0_`basename $file` template.hdr
done

$SYNAPSE_PROFILE mImgtbl   projdir images.tbl
$SYNAPSE_PROFILE mOverlaps images.tbl diffs.tbl

NAPSE_PROFILE="echo"
NAPSE_PROFILE=""
while read v1 v2 v3 v4 v5
do           
    if test "$v1" = "|"
    then
        continue
    fi

    V1=`printf "%0.6d" "$v1"`
    V2=`printf "%0.6d" "$v2"`
    cmd2="mDiff     -n projdir/$v3 projdir/$v4 diffdir/diff.$V1.$V2.fits template.hdr"
    cmd3="mFitplane -b 0 diffdir/diff.$V1.$V2.fits -s statdir/stats-diff.$V1.$V2.fits"
    $SYNAPSE_PROFILE $cmd2
    $SYNAPSE_PROFILE $cmd3
done < diffs.tbl

for file in statdir/*
do
    echo $file
    cat  $file | grep -v 'command:'
done > stats.tbl

$SYNAPSE_PROFILE python   format-fits.py stats.tbl fits.tbl
$SYNAPSE_PROFILE mBgModel images.tbl fits.tbl corrections.tbl

while read v1 v2 v3 v4
do           
    if test "$v1" = "|"
    then
        continue
    fi

    par="$v2 $v3 $v4"
    img="`grep -e "^ *$v1 "  images.tbl | tr -s ' ' | cut -f 25 -d ' '`"
    outf="projdir/`basename $img`"
    out="corrdir/`basename $img`"

    echo mBackground $outf $out $par
    $SYNAPSE_PROFILE mBackground $outf $out $par

done < corrections.tbl

$SYNAPSE_PROFILE mImgtbl  corrdir corr-images.tbl
$SYNAPSE_PROFILE mAdd     -n -p corrdir corr-images.tbl template.hdr final/m101_corrected.fits
$SYNAPSE_PROFILE mJPEG    -gray final/m101_corrected.fits 0s max gaussian-log -out final/m101_corrected.jpg

touch cleanme

