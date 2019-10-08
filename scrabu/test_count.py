testlist = ["1Alex", "Alex", "Alex", "Burak", "Burak", "Burak", "Cong", "Cong", "Cong", "Dariush", "Dariush", "Dariush",
            "Sergej", "Sergej", "Sergej"]

listcount = dict((i, testlist.count(i)) for i in testlist)

print(listcount)
