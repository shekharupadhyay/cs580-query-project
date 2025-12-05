"""
Problem 4: Create random dataset and compare Problem 2 vs Problem 3

Dataset:
- R1: 100 tuples (i, x) where i=1..100, x is random in [1, 5000]
- R2: 100 tuples (y, j) where j=1..100, y is random in [1, 5000]
- R3: 100 tuples (ℓ, ℓ) where ℓ=1..100

Compare Yannakakis (Problem 2) vs Naive Join (Problem 3)
"""

import random
import time
from typing import List, Tuple
from problem2_yannakakis import yannakakis_line_join
from problem3_naive_join import naive_line_join


def generate_random_dataset() -> Tuple[List[Tuple[int, int]], List[Tuple[int, int]], List[Tuple[int, int]]]:
    """Generate random dataset as specified in Problem 4"""
    random.seed(42)

    R1 = [(i, random.randint(1, 5000)) for i in range(1, 101)]

    R2 = [(random.randint(1, 5000), j) for j in range(1, 101)]

    R3 = [(l, l) for l in range(1, 101)]

    return R1, R2, R3


def main():
    print("=" * 80)
    print("Problem 4: Random Dataset Testing")
    print("=" * 80)

    R1, R2, R3 = generate_random_dataset()

    print(f"\nDataset sizes:")
    print(f"  R1: {len(R1)} tuples")
    print(f"  R2: {len(R2)} tuples")
    print(f"  R3: {len(R3)} tuples")

    print(f"\nSample R1 (first 5): {R1[:5]}")
    print(f"Sample R2 (first 5): {R2[:5]}")
    print(f"Sample R3 (first 5): {R3[:5]}")

    print("\n" + "-" * 80)
    print("Running Yannakakis Algorithm (Problem 2)...")
    start_time = time.time()
    result_yannakakis = yannakakis_line_join([R1, R2, R3])
    yannakakis_time = time.time() - start_time
    print(f"Yannakakis completed in {yannakakis_time:.6f} seconds")
    print(f"Number of results: {len(result_yannakakis)}")

    print("\n" + "-" * 80)
    print("Running Naive Join Algorithm (Problem 3)...")
    start_time = time.time()
    result_naive = naive_line_join([R1, R2, R3])
    naive_time = time.time() - start_time
    print(f"Naive Join completed in {naive_time:.6f} seconds")
    print(f"Number of results: {len(result_naive)}")

    print("\n" + "-" * 80)
    print("Comparison:")
    print(f"  Yannakakis time: {yannakakis_time:.6f} seconds")
    print(f"  Naive Join time: {naive_time:.6f} seconds")
    print(f"  Speedup: {naive_time / yannakakis_time:.2f}x")

    result_yannakakis_sorted = sorted(result_yannakakis)
    result_naive_sorted = sorted(result_naive)

    if result_yannakakis_sorted == result_naive_sorted:
        print("\n✓ Results match! Both algorithms produce the same output.")
    else:
        print("\n✗ Results differ!")
        print(f"  Yannakakis result count: {len(result_yannakakis)}")
        print(f"  Naive join result count: {len(result_naive)}")

    if result_yannakakis_sorted:
        print(f"\nSample results (first 10):")
        for i, t in enumerate(result_yannakakis_sorted[:10]):
            print(f"  {t}")
    else:
        print("\nNo results found (empty join)")

    print("\n" + "=" * 80)
    print("Analysis:")
    print("=" * 80)
    print("""
The dataset has:
- R1(A1, A2): 100 tuples where A1 is sequential (1-100) and A2 is random [1, 5000]
- R2(A2, A3): 100 tuples where A2 is random [1, 5000] and A3 is sequential (1-100)
- R3(A3, A4): 100 tuples where both A3 and A4 are the same (1-100)

The join query is: q(A1, A2, A3, A4) :- R1(A1, A2), R2(A2, A3), R3(A3, A4)

For a tuple to appear in the result:
1. R1.A2 must match R2.A2 (random values must coincide)
2. R2.A3 must match R3.A3 (R2.A3 ∈ [1, 100] and R3.A3 ∈ [1, 100])

Since R1.A2 and R2.A2 are both random in [1, 5000], the probability of a match
is relatively low. However, R3 ensures that R2.A3 values in [1, 100] will have
matches.

Both algorithms should produce identical results. The Yannakakis algorithm
performs semi-join reductions first, which eliminates non-joining tuples early,
potentially reducing the intermediate result sizes. The naive approach joins
sequentially without reduction, possibly creating larger intermediate results.
""")


if __name__ == "__main__":
    main()
