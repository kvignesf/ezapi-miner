def contains_in_list(A, B):
    found = False
    for b in B:
        if b in A:
            found = True
    return found