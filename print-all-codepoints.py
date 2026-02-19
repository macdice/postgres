for i in range(0, 0x10ffff + 1):
    if i >= 0xd800 and i <= 0xdfff:
        continue # skip UTF-16 surrogates, which have no valid encoding in UTF-8
    print(chr(i))
