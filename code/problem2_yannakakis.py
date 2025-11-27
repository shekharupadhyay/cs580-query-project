from collections import defaultdict
from typing import List, Tuple


Tuple2 = Tuple[int, int]
TupleN = Tuple[int, ...]




def hash_join_generic(
    left: List[TupleN],
    right: List[TupleN],
    left_key_index: int,
    right_key_index: int,
) -> List[TupleN]:

    hash_right = defaultdict(list)
    for t in right:
        key = t[right_key_index]
        hash_right[key].append(t)

    result: List[TupleN] = []

    for lt in left:
        key = lt[left_key_index]
        if key in hash_right:
            for rt in hash_right[key]:
                merged = lt + rt[:right_key_index] + rt[right_key_index + 1 :]
                result.append(merged)

    return result




def forward_semijoin(relations: List[List[Tuple2]]) -> List[List[Tuple2]]:
    
    k = len(relations)
    # Make a copy to avoid mutating input directly
    R = [list(rel) for rel in relations]

    
    for i in range(k - 2, -1, -1):
        right = R[i + 1]
        # Collect the set of valid join keys from right's first attribute
        valid_keys = {t[0] for t in right}  # A_{i+1} from R_{i+1}
        # Filter R_i
        left = R[i]
        R[i] = [t for t in left if t[1] in valid_keys]  # keep tuples whose A_{i+1} is in valid_keys

    return R


def backward_semijoin(relations: List[List[Tuple2]]) -> List[List[Tuple2]]:
    
    k = len(relations)
    R = [list(rel) for rel in relations]

    # From R_2 up to R_k
    for i in range(1, k):
        left = R[i - 1]
        
        valid_keys = {t[1] for t in left}
        right = R[i]
        
        R[i] = [t for t in right if t[0] in valid_keys]

    return R


def yannakakis_line_join(relations: List[List[Tuple2]]) -> List[TupleN]:
    
    if not relations:
        return []

    
    R_fwd = forward_semijoin(relations)

    
    R_red = backward_semijoin(R_fwd)

    
    current: List[TupleN] = [tuple(t) for t in R_red[0]]

    if len(R_red) == 1:
        return current

    for i in range(1, len(R_red)):
        next_rel = [tuple(t) for t in R_red[i]]
        if not current or not next_rel:
            return []
        
        current = hash_join_generic(
            current,
            next_rel,
            left_key_index=len(current[0]) - 1,
            right_key_index=0,
        )

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

    print("Original R1 (A1, A2):")
    for t in R1:
        print("  ", t)

    print("\nOriginal R2 (A2, A3):")
    for t in R2:
        print("  ", t)

    print("\nOriginal R3 (A3, A4):")
    for t in R3:
        print("  ", t)

    result = yannakakis_line_join(relations)

    print("\nYannakakis 3-line join result q(A1, A2, A3, A4):")
    for t in result:
        print("  ", t)


if __name__ == "__main__":
    main()
