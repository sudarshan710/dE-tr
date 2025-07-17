square = [x**2 for x in range(7)]
evens = [x for x in range(10) if x%2==0]
print('\n', square, evens)

sq = list(map(lambda x: x**2, range(7)))
ev = list(filter(lambda x: x%2==0 , range(10)))
print(sq, '\n', ev)