#!/usr/bin/gnuplot

reset

unset tics
set term png
set output "partitionsample.png"

#plot 'plot.dat' using (($2+$4)/2):(($3+$5)/2):(($4-$2)/2):(($5-$3)/2) \
#    w boxxyerrorbars lw 1 notitle

load "vis.dem"


