#! /usr/bin/python

import sys
import math
import random

# Generate random polygons in a space obtained 
#             via command line arguments (first 4 arguments)
#  The 5-th command line argument is 
#     the number of objects to be generated
#  The 6-th argument is the number of other random attributes
#  The 7-th argument is the approximate desirable
#                 size of each object
# The output should be tab-separated
# Object ID will be the first field, followed by 
# 

def getRandCoord(min, max, obj_size):
    return min + (max - min) * (random.random())

def roundNum(num):
    return str(round(num, 4))

def generateGeom(min_x, min_y, max_x, max_y, obj_size):
    poly = "POLYGON(("
    start_x = getRandCoord(min_x, max_x, obj_size)
    start_y = getRandCoord(min_y, max_y, obj_size)

    poly += roundNum(start_x) + " " + roundNum(start_y)
    x2 = start_x + (random.random() * 0.5 + 0.5 ) * obj_size
    y2 = start_y + (random.random() * 0.5 + 0.5 ) * obj_size
    poly += "," + roundNum(x2) + " " + roundNum(y2)

    x3 = start_x + (random.random() * 0.5 + 0.5 ) * obj_size
    y3 = start_y + (random.random() * 0.5 + 0.5 ) * obj_size

    poly += "," + roundNum(x3) + " " + roundNum(y3) + "," + roundNum(start_x) + " " + roundNum(start_y) + "))"
    return poly

def main():
    if len(sys.argv) != 8:
        print("Not enough arguments (7): [min_x] [min_y] [max_x] [max_y] [num_obj] [number spatialattr] [obj_size]")
        sys.exit(1)
    space_min_x = float(sys.argv[1])
    space_min_y = float(sys.argv[2])
    space_max_x = float(sys.argv[3])
    space_max_y = float(sys.argv[4])
    space_x_span = space_max_x - space_min_x
    space_y_span = space_max_y - space_min_y

    num_objects = int(sys.argv[5])
    num_non_spatial_attr = int(sys.argv[6])
    obj_size = float(sys.argv[7])

    id = 1
    for x in range(0, num_objects):
        geom = generateGeom(space_min_x, space_min_y, space_max_y, space_max_y, obj_size)
        nonspattr = ""
        for t in range(0, num_non_spatial_attr):
            nonspattr += "\t" + str(int(round(random.random() * 8, 0)))
        print(str(id) + "\t" + geom + nonspattr)
        id = id + 1

    sys.stdout.flush()
    sys.stderr.flush()

if __name__ == '__main__':
    main()

