from collections import defaultdict
from typing import List, Tuple, Dict


TupleAB = Tuple[int, int]
TupleBC = Tuple[int, int]
TupleABC = Tuple[int, int, int]


def build_hash_on_R2(R2: List[TupleBC]) -> Dict[int, List[TupleBC]]:
    
    h: Dict[int, List[TupleBC]] = defaultdict(list)
    for (b, c) in R2:
        h[b].append((b, c))
    return h


def hash_join(R1: List[TupleAB], R2: List[TupleBC]) -> List[TupleABC]:

    
    hash_R2 = build_hash_on_R2(R2)

    
    result: List[TupleABC] = []
    for (a, b) in R1:
        if b in hash_R2:
            for (_b, c) in hash_R2[b]:
                
                result.append((a, b, c))
    return result


def main():
   
    R1: List[TupleAB] = [
        (1, 10),
        (2, 20),
        (3, 10),
        (4, 30),
        (5, 40),
        (6, 50),
        (7, 20),
        (8, 60),
        (9, 70),
        (10, 80),
    ]

    
    R2: List[TupleBC] = [
        (10, 100),
        (10, 101),   
        (20, 200),
        (30, 300),
        (30, 301),   
        (50, 500),
        (90, 900),   
        (100, 1000), 
        (60, 600),
        (70, 700),
    ]

    print("R1 (A, B):")
    for t in R1:
        print("  ", t)

    print("\nR2 (B, C):")
    for t in R2:
        print("  ", t)

    
    result = hash_join(R1, R2)

    print("\nJoin result q(A, B, C) = R1(A, B) ⨝ R2(B, C):")
    for t in result:
        print("  ", t)


if __name__ == "__main__":
    main()
