#!/usr/bin/env python3
import sys
import os

handle=open(sys.argv[1], "r")
output=open(sys.argv[2], "w+")
output.write("| plus|minus|       a    |      b     |      c     | crpix1  | crpix2  | xmin | xmax | ymin | yma\
x | xcenter | ycenter |  npixel |    rms     |    boxx    |    boxy    |  boxwidth  | boxheight  |   boxang   |\n")

while True:
    line=handle.readline()
    print line
    if not line:
        break
    eles=line.split(".")

    plus=int(eles[1])
    minus=int(eles[2])
    #lineid+=1
    #outline=("%.6d%.6d" % (plus, minus))

    line=handle.readline()
    if not line:
        break
    outline2=""
    eles=line.split(",")
    if len(eles) < 19:
        continue
    subeles=eles[1].split("=")
    a=float(subeles[1])

    subeles=eles[2].split("=")
    b=float(subeles[1])

    subeles=eles[3].split("=")
    c=float(subeles[1])

    subeles=eles[4].split("=")
    c1=float(subeles[1])

    subeles=eles[5].split("=")
    c2=float(subeles[1])

    subeles=eles[6].split("=")
    xmin=int(subeles[1])

    subeles=eles[7].split("=")
    xmax=int(subeles[1])

    subeles=eles[8].split("=")
    ymin=int(float(subeles[1]))

    subeles=eles[9].split("=")
    ymax=int(float(subeles[1]))
    
    subeles=eles[10].split("=")
    xcenter=float(subeles[1])

    subeles=eles[11].split("=")
    ycenter=float(subeles[1])

    subeles=eles[12].split("=")
    npixel=int(subeles[1])
    
    subeles=eles[13].split("=")
    rms=float(subeles[1])

    subeles=eles[14].split("=")
    boxx=float(subeles[1])

    subeles=eles[15].split("=")
    boxy=float(subeles[1])
    
    subeles=eles[16].split("=")
    boxwidth=float(subeles[1])

    subeles=eles[17].split("=")
    boxheight=float(subeles[1])

    subeles=eles[18].split("=")
    boxang=float(subeles[1][:len(subeles[1])-2])

    output.write(" %5d %5d %12.5e %12.5e %12.5e %9.2f %9.2f %6d %6d %6d %6d %9.2f %9.2f %9.0f %12.5e %12.1f %12.1f %12.1f %12.1f %12.1f\n" % (plus, minus, a, b, c, c1, c2, xmin, xmax, ymin, ymax, xcenter, ycenter, npixel, rms, boxx, boxy, boxwidth, boxheight, boxang))
handle.close()
output.close()

