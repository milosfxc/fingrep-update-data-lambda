from edgar import Company, XBRL

if __name__ == '__main__':
    c = [2, None, 2]
    t = None
    for i in c:
        if i:
            t += i

    print(t)