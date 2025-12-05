

from collections import defaultdict
from typing import List, Tuple


def hash_join_generic(
    left: List[Tuple[int, ...]],
    right: List[Tuple[int, ...]],
    left_key_index: int,
    right_key_index: int,
) -> List[Tuple[int, ...]]:
    
   
    hash_right = defaultdict(list)
    for t in right:
        key = t[right_key_index]
        hash_right[key].append(t)

    result: List[Tuple[int, ...]] = []

    for lt in left:
        key = lt[left_key_index]
        if key in hash_right:
            for rt in hash_right[key]:
                merged = lt + rt[:right_key_index] + rt[right_key_index + 1 :]
                result.append(merged)

    return result


def naive_line_join(relations: List[List[Tuple[int, int]]]) -> List[Tuple[int, ...]]:
    
    if not relations:
        return []

    current = relations[0]

    
    if len(relations) == 1:
        return [tuple(t) for t in current]

    
    for i in range(1, len(relations)):
        next_rel = relations[i]
        
        current = hash_join_generic(
            current,
            next_rel,
            left_key_index=len(current[0]) - 1,
            right_key_index=0,
        )

        
        if not current:
            break

    return current


def main():
   
    R1 = [
        (1, 10),
        (2, 20),
        (3, 30),
        (4, 40),
    ]

    
    R2 = [
        (10, 100),
        (20, 200),
        (50, 500),  
    ]

    R3 = [
        (100, 1000),
        (200, 2000),
        (300, 3000),  
    ]

    relations = [R1, R2, R3]

    print("R1 (A1, A2):")
    for t in R1:
        print("  ", t)

    print("\nR2 (A2, A3):")
    for t in R2:
        print("  ", t)

    print("\nR3 (A3, A4):")
    for t in R3:
        print("  ", t)

    result = naive_line_join(relations)

    print("\nNaive 3-line join result q(A1, A2, A3, A4):")
    for t in result:
        print("  ", t)


if __name__ == "__main__":
    main()
