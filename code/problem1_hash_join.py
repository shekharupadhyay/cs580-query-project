"""
CS580 – Project (Option iv)
Problem 1: Implement hash-join for q(A, B, C) :- R1(A, B), R2(B, C)

We:
- Build a hash map on R2 using attribute B as key
- For each tuple in R1, probe the hash map on its B value
- Output all (A, B, C) triples that satisfy the join condition
"""

from collections import defaultdict
from typing import List, Tuple, Dict


# Type aliases for clarity
TupleAB = Tuple[int, int]
TupleBC = Tuple[int, int]
TupleABC = Tuple[int, int, int]


def build_hash_on_R2(R2: List[TupleBC]) -> Dict[int, List[TupleBC]]:
    """
    Build a hash map on relation R2(B, C) with:
        key   = B value
        value = list of tuples (B, C) that share that B

    h[b] = [ (b, c1), (b, c2), ... ]
    """
    h: Dict[int, List[TupleBC]] = defaultdict(list)
    for (b, c) in R2:
        h[b].append((b, c))
    return h


def hash_join(R1: List[TupleAB], R2: List[TupleBC]) -> List[TupleABC]:
    """
    Evaluate q(A, B, C) :- R1(A, B), R2(B, C) using a hash join.

    Steps:
    1. Build a hash map on R2 using B as key.
    2. For each (a, b) in R1, probe the hash map using b.
    3. For each match (b, c) in R2, output (a, b, c).
    """
    # Step 1: build hash index on R2
    hash_R2 = build_hash_on_R2(R2)

    # Step 2 & 3: probe and output join results
    result: List[TupleABC] = []
    for (a, b) in R1:
        if b in hash_R2:
            for (_b, c) in hash_R2[b]:
                # Here _b == b, but we keep it explicit for clarity
                result.append((a, b, c))
    return result


def main():
    # ------------------------------------------------------------------
    # Example dataset with 10 tuples in each relation (as required)
    # R1(A, B)
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

    # R2(B, C)
    R2: List[TupleBC] = [
        (10, 100),
        (10, 101),   # multiple matches for B = 10
        (20, 200),
        (30, 300),
        (30, 301),   # multiple matches for B = 30
        (50, 500),
        (90, 900),   # no match in R1
        (100, 1000), # no match in R1
        (60, 600),
        (70, 700),
    ]

    print("R1 (A, B):")
    for t in R1:
        print("  ", t)

    print("\nR2 (B, C):")
    for t in R2:
        print("  ", t)

    # Run hash join
    result = hash_join(R1, R2)

    print("\nJoin result q(A, B, C) = R1(A, B) ⨝ R2(B, C):")
    for t in result:
        print("  ", t)


if __name__ == "__main__":
    main()
