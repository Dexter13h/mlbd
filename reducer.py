#!/usr/bin/env python3
import sys

res, res_sq, cnt = 0, 0, 0

for line in sys.stdin:
    c_i, sum_i, sum_sq_i = [float(x) for x in line.split()]
    cnt += c_i
    res += sum_i
    res_sq += sum_sq_i

print(res / cnt)
print((res_sq - res * res / cnt) / cnt)
