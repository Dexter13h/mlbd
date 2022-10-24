#!/usr/bin/env python3

import sys
import csv

res, res_sq, cnt = 0, 0, 0
for line in sys.stdin:
    try:
        price = float(line)
        res += price
        res_sq += price * price
        cnt += 1
    except:
        pass

print(price_cnt, price_sum, price2_sum)
