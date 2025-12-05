"""
Problem 5: Create specific dataset and compare Problem 2 vs Problem 3

Dataset:
R1:
- 1000 tuples (i, 5) for i=1..1000
- 1000 tuples (i, 7) for i=1001..2000
- 1 tuple (2001, 2002)
- Apply random permutation

R2:
- 1000 tuples (5, i) for i=1..1000
- 1000 tuples (7, i) for i=1001..2000
- 1 tuple (2002, 8)
- Apply random permutation

R3:
- 2000 random tuples (x, y) with x ∈ [2002, 3000] and y ∈ [1, 3000]
- 1 tuple (8, 30)
- Apply random permutation

Compare Yannakakis (Problem 2) vs Naive Join (Problem 3)
"""

import random
import time
from typing import List, Tuple
from problem2_yannakakis import yannakakis_line_join
from problem3_naive_join import naive_line_join


def generate_specific_dataset() -> Tuple[List[Tuple[int, int]], List[Tuple[int, int]], List[Tuple[int, int]]]:
    """Generate specific dataset as described in Problem 5"""
    random.seed(42)

    R1 = []
    R1.extend([(i, 5) for i in range(1, 1001)])
    R1.extend([(i, 7) for i in range(1001, 2001)])
    R1.append((2001, 2002))
    random.shuffle(R1)

    R2 = []
    R2.extend([(5, i) for i in range(1, 1001)])
    R2.extend([(7, i) for i in range(1001, 2001)])
    R2.append((2002, 8))
    random.shuffle(R2)

    R3 = []
    R3.extend([
        (random.randint(2002, 3000), random.randint(1, 3000))
        for _ in range(2000)
    ])
    R3.append((8, 30))
    random.shuffle(R3)

    return R1, R2, R3


def main():
    print("=" * 80)
    print("Problem 5: Specific Dataset Testing")
    print("=" * 80)

    R1, R2, R3 = generate_specific_dataset()

    print(f"\nDataset sizes:")
    print(f"  R1: {len(R1)} tuples")
    print(f"  R2: {len(R2)} tuples")
    print(f"  R3: {len(R3)} tuples")

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

    if yannakakis_time > 0:
        speedup = naive_time / yannakakis_time
        print(f"  Speedup factor: {speedup:.2f}x")
        if speedup > 1:
            print(f"  → Yannakakis is {speedup:.2f}x FASTER")
        else:
            print(f"  → Naive is {1/speedup:.2f}x FASTER")

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
        if len(result_yannakakis_sorted) > 10:
            print(f"  ... and {len(result_yannakakis_sorted) - 10} more")
    else:
        print("\nNo results found (empty join)")

    print("\n" + "=" * 80)
    print("Analysis:")
    print("=" * 80)
    print("""
This dataset is designed to create a specific join pattern:

R1 has three groups:
  - 1000 tuples with A2=5 (i=1..1000)
  - 1000 tuples with A2=7 (i=1001..2000)
  - 1 tuple with A2=2002 (i=2001)

R2 has three groups:
  - 1000 tuples with A2=5 (A3=1..1000)
  - 1000 tuples with A2=7 (A3=1001..2000)
  - 1 tuple with A2=2002 (A3=8)

R3 has:
  - 2000 random tuples with A3 ∈ [2002, 3000]
  - 1 tuple (8, 30)

Join chain: R1(A1,A2) ⋈ R2(A2,A3) ⋈ R3(A3,A4)

Expected join behavior:
1. R1 ⋈ R2 on A2:
   - 1000 tuples from R1 (A2=5) × 1000 tuples from R2 (A2=5) = 1,000,000 intermediate tuples
   - 1000 tuples from R1 (A2=7) × 1000 tuples from R2 (A2=7) = 1,000,000 intermediate tuples
   - 1 tuple from R1 (A2=2002) × 1 tuple from R2 (A2=2002) = 1 intermediate tuple
   - Total: 2,000,001 intermediate tuples

2. (R1⋈R2) ⋈ R3 on A3:
   - From the 2M intermediate tuples, only those with A3 ∈ {1..2000, 8} can join with R3
   - R3 only has A3=8 that matches (plus random values ≥2002)
   - Only the tuple (..., 8) from R1⋈R2 will join with R3's (8, 30)
   - This comes from the chain: (2001, 2002) from R1 → (2002, 8) from R2 → (8, 30) from R3

Yannakakis advantage:
- Forward semi-join: Reduces R1 and R2 by eliminating tuples that won't join with R3
  - Since R3 has mostly A3 ∈ [2002, 3000] plus one A3=8, it filters R2 to keep only A3=8
  - This drastically reduces R1⋈R2 size before the join happens

Naive approach:
- Computes full R1⋈R2 first (2,000,001 tuples!)
- Then filters by joining with R3

This demonstrates why Yannakakis is asymptotically better for queries with small outputs
but potentially large intermediate results.
""")

    if yannakakis_time > 0 and naive_time > 0:
        print(f"\nPerformance Summary:")
        print(f"  Yannakakis reduces data early via semi-joins: {yannakakis_time:.6f}s")
        print(f"  Naive computes large intermediate results: {naive_time:.6f}s")
        if speedup > 1:
            print(f"  Yannakakis is {speedup:.2f}x faster due to early pruning!")


if __name__ == "__main__":
    main()
