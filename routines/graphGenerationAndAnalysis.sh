#!/bin/bash

#!/usr/bin/env python

x=$1
y=$2
d=$3
n=$4
u=$5
p=$6
M=$7
t=$8
a=$9
echo "creation of the Junction graph"
python crateJunctionGraph.py -x "$x" -y "$y" -d "$d" -n "$n" -u "$u" -p "$p" -f "$M"
echo "importing traffic informations from" $t
python traffic.py -n "$n" -u "$u" -p "$p" -f "$t"
echo "creation of the Road Section graph"
python crateRoadSectionGraph.py -n "$n" -u "$u" -p "$p"
echo "Some information about the graphs"
python graphAnalysis.py -n "$n" -u "$u" -p "$p"
python graphAnalysis.py -n "$n" -u "$u" -p "$p"
echo "Look in your browser: a map with the 100 most important junctions will be displayed"
python algorithmAppliedToJunctionsAndRoads.py -x "$x" -y "$y" -n "$n" -u "$u" -p "$p" -f "$a"-a 1
echo "Look in your browser: a map with the 100 most important junctions considering traffic will be displayed"
python algorithmAppliedToJunctionsAndRoads.py -x "$x" -y "$y" -n "$n" -u "$u" -p "$p" -f "$a"-a 2
echo "Look in your browser: a map with the roads located in several community and thus influential considering the graph topology are shown."
python algorithmAppliedToJunctionsAndRoads.py -x "$x" -y "$y" -n "$n" -u "$u" -p "$p" -f "$a"-a 3
echo "Look in your browser: a map with the roads with the highest page rank score considering traffic are displayed."
python algorithmAppliedToJunctionsAndRoads.py -x "$x" -y "$y" -n "$n" -u "$u" -p "$p" -f "$a"-a 4
