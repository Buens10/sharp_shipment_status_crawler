a = [[1, 2], [3, 4]]
abc = []
for i in a:
    b = list(map(lambda x: x*2, i))

    for j in b:
        c = j * 2
        abc.append(c)

print(abc)

