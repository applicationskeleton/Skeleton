if [ "$#" -ne 1 ];then
    echo "wrong input format"
    echo "example: ./plot.sh map-reduce.dax"
    exit 1
fi

input=$1
prefix=`echo ${input}| cut -d '.' -f 1` 

path=../src/Pegasus/dax2dot

if [ "${path}" == "" ];then
    echo "pegasus is not properly installed"
    echo "need to run: export PATH=${PATH}:/path/to/dax2dot "
    exit 1
fi

${path} -f ${input} -o ${prefix}.dot
dot -Tpng ${prefix}.dot -o ${prefix}.png
