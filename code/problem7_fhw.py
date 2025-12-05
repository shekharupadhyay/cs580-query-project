"""
Problem 7: Fractional Hypertree Width (FHW) Algorithm

Query: q(A1, A2, A3, A4, A5, A6) :- R1(A1, A2), R2(A2, A3), R3(A1, A3),
                                     R4(A3, A4), R5(A4, A5), R6(A5, A6), R7(A4, A6)

Fractional Hypertree Decomposition:
- Uses fractional edge covers instead of exact covers
- Running time: O(N^(ρ*+1) + OUT) where ρ* is the fractional hypertree width
- More fine-grained than GHW, can be strictly smaller
- ρ* ≤ fhw ≤ ghw
"""

import csv
import time
from collections import defaultdict
from typing import List, Tuple, Dict, Set


def load_relation_from_csv(filepath: str) -> List[Tuple[int, ...]]:
    """Load a relation from CSV file"""
    with open(filepath, 'r') as f:
        reader = csv.reader(f)
        header = next(reader)
        return [tuple(int(x) for x in row) for row in reader]


class FHWJoin:
    """
    Fractional Hypertree Width Join Algorithm

    For this specific query, we use fractional edge covers to guide the join order.
    The fractional edge cover gives weights to relations to "cover" each variable.

    For a set of variables X, the fractional edge cover ρ(X) is the minimum value
    such that we can assign weights w_R to each relation R where:
    - Sum of w_R ≤ ρ(X)
    - For each variable v ∈ X, sum of w_R over relations containing v ≥ 1

    The FHW is the maximum ρ(χ(t)) over all bags χ(t) in the decomposition.
    """

    def __init__(self, relations: Dict[str, List[Tuple[int, ...]]],
                 schemas: Dict[str, List[str]]):
        self.relations = relations
        self.schemas = schemas

    def _hash_join(self, left: List[Tuple], left_schema: List[str],
                   right: List[Tuple], right_schema: List[str]) -> Tuple[List[Tuple], List[str]]:
        """Hash join two relations on common attributes"""
        common_attrs = [attr for attr in left_schema if attr in right_schema]

        if not common_attrs:
            result = [(l + r) for l in left for r in right]
            result_schema = left_schema + right_schema
            return result, result_schema

        left_indices = [left_schema.index(attr) for attr in common_attrs]
        right_indices = [right_schema.index(attr) for attr in common_attrs]

        hash_table = defaultdict(list)
        for r_tuple in right:
            key = tuple(r_tuple[i] for i in right_indices)
            hash_table[key].append(r_tuple)

        result = []
        for l_tuple in left:
            key = tuple(l_tuple[i] for i in left_indices)
            if key in hash_table:
                for r_tuple in hash_table[key]:
                    merged = list(l_tuple)
                    for i, attr in enumerate(right_schema):
                        if attr not in left_schema:
                            merged.append(r_tuple[i])
                    result.append(tuple(merged))

        result_schema = list(left_schema)
        for attr in right_schema:
            if attr not in result_schema:
                result_schema.append(attr)

        return result, result_schema

    def _semijoin_reduce(self, rel1: List[Tuple], schema1: List[str],
                         rel2: List[Tuple], schema2: List[str]) -> List[Tuple]:
        """
        Semi-join reduction: Keep only tuples from rel1 that join with rel2

        Returns: Reduced rel1
        """
        common_attrs = [attr for attr in schema1 if attr in schema2]

        if not common_attrs:
            return rel1

        rel1_indices = [schema1.index(attr) for attr in common_attrs]
        rel2_indices = [schema2.index(attr) for attr in common_attrs]

        rel2_keys = set()
        for t in rel2:
            key = tuple(t[i] for i in rel2_indices)
            rel2_keys.add(key)

        result = []
        for t in rel1:
            key = tuple(t[i] for i in rel1_indices)
            if key in rel2_keys:
                result.append(t)

        return result

    def execute(self) -> List[Tuple[int, ...]]:
        """
        Execute query using FHW-guided approach

        Strategy:
        1. Use semi-join reductions to prune relations
        2. Join in an order that respects fractional edge covers
        3. For this query, use a smart ordering based on the structure

        For our query, the fractional edge cover analysis suggests:
        - Join triangle first (R1, R2, R3) with careful ordering
        - Then extend to R4
        - Handle diamond (R5, R6, R7) efficiently
        """

        print("  Step 1: Semi-join reductions on triangle {R1, R2, R3}...")

        R1_red = list(self.relations['R1'])
        R2_red = list(self.relations['R2'])
        R3_red = list(self.relations['R3'])

        R1_red = self._semijoin_reduce(R1_red, self.schemas['R1'],
                                       R2_red, self.schemas['R2'])
        R1_red = self._semijoin_reduce(R1_red, self.schemas['R1'],
                                       R3_red, self.schemas['R3'])

        R2_red = self._semijoin_reduce(R2_red, self.schemas['R2'],
                                       R1_red, self.schemas['R1'])
        R2_red = self._semijoin_reduce(R2_red, self.schemas['R2'],
                                       R3_red, self.schemas['R3'])

        R3_red = self._semijoin_reduce(R3_red, self.schemas['R3'],
                                       R1_red, self.schemas['R1'])
        R3_red = self._semijoin_reduce(R3_red, self.schemas['R3'],
                                       R2_red, self.schemas['R2'])

        print(f"    R1: {len(self.relations['R1'])} -> {len(R1_red)} tuples")
        print(f"    R2: {len(self.relations['R2'])} -> {len(R2_red)} tuples")
        print(f"    R3: {len(self.relations['R3'])} -> {len(R3_red)} tuples")

        print("  Step 2: Join triangle...")
        temp1, schema1 = self._hash_join(R1_red, self.schemas['R1'],
                                         R2_red, self.schemas['R2'])
        temp1, schema1 = self._hash_join(temp1, schema1,
                                         R3_red, self.schemas['R3'])
        print(f"    Triangle result: {len(temp1)} tuples")

        print("  Step 3: Semi-join R4 with triangle...")
        R4_red = self._semijoin_reduce(
            list(self.relations['R4']), self.schemas['R4'],
            temp1, schema1
        )
        print(f"    R4: {len(self.relations['R4'])} -> {len(R4_red)} tuples")

        print("  Step 4: Join with R4...")
        temp2, schema2 = self._hash_join(temp1, schema1,
                                         R4_red, self.schemas['R4'])
        print(f"    Result: {len(temp2)} tuples")

        print("  Step 5: Semi-join reductions on diamond {R5, R6, R7}...")

        R5_red = self._semijoin_reduce(
            list(self.relations['R5']), self.schemas['R5'],
            temp2, schema2
        )

        R7_red = self._semijoin_reduce(
            list(self.relations['R7']), self.schemas['R7'],
            temp2, schema2
        )

        R5_red = self._semijoin_reduce(R5_red, self.schemas['R5'],
                                       R7_red, self.schemas['R7'])
        R7_red = self._semijoin_reduce(R7_red, self.schemas['R7'],
                                       R5_red, self.schemas['R5'])

        R6_red = self._semijoin_reduce(
            list(self.relations['R6']), self.schemas['R6'],
            R5_red, self.schemas['R5']
        )

        print(f"    R5: {len(self.relations['R5'])} -> {len(R5_red)} tuples")
        print(f"    R6: {len(self.relations['R6'])} -> {len(R6_red)} tuples")
        print(f"    R7: {len(self.relations['R7'])} -> {len(R7_red)} tuples")

        print("  Step 6: Join diamond (R5, R7 first)...")
        temp3_sub, schema3_sub = self._hash_join(
            R5_red, self.schemas['R5'],
            R7_red, self.schemas['R7']
        )
        print(f"    R5 ⋈ R7: {len(temp3_sub)} tuples")

        print("  Step 7: Join with current result...")
        temp3, schema3 = self._hash_join(temp2, schema2,
                                         temp3_sub, schema3_sub)
        print(f"    Result: {len(temp3)} tuples")

        print("  Step 8: Join with R6...")
        temp4, schema4 = self._hash_join(temp3, schema3,
                                         R6_red, self.schemas['R6'])
        print(f"    Final result: {len(temp4)} tuples")

        output_attrs = ['A1', 'A2', 'A3', 'A4', 'A5', 'A6']
        indices = [schema4.index(attr) for attr in output_attrs]

        result = []
        seen = set()
        for t in temp4:
            proj = tuple(t[i] for i in indices)
            if proj not in seen:
                seen.add(proj)
                result.append(proj)

        return result


def main():
    print("=" * 80)
    print("Problem 7: Fractional Hypertree Width (FHW) Algorithm")
    print("=" * 80)

    print("\nQuery:")
    print("q(A1, A2, A3, A4, A5, A6) :- R1(A1, A2), R2(A2, A3), R3(A1, A3),")
    print("                              R4(A3, A4), R5(A4, A5), R6(A5, A6), R7(A4, A6)")

    import os
    current_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(current_dir)
    base_path = os.path.join(project_root, "query_relations") + "/"

    relations = {
        'R1': load_relation_from_csv(base_path + 'R1.csv'),
        'R2': load_relation_from_csv(base_path + 'R2.csv'),
        'R3': load_relation_from_csv(base_path + 'R3.csv'),
        'R4': load_relation_from_csv(base_path + 'R4.csv'),
        'R5': load_relation_from_csv(base_path + 'R5.csv'),
        'R6': load_relation_from_csv(base_path + 'R6.csv'),
        'R7': load_relation_from_csv(base_path + 'R7.csv'),
    }

    schemas = {
        'R1': ['A1', 'A2'],
        'R2': ['A2', 'A3'],
        'R3': ['A1', 'A3'],
        'R4': ['A3', 'A4'],
        'R5': ['A4', 'A5'],
        'R6': ['A5', 'A6'],
        'R7': ['A4', 'A6'],
    }

    print(f"\nDataset sizes:")
    for name in sorted(relations.keys()):
        print(f"  {name}: {len(relations[name])} tuples")

    print("\n" + "-" * 80)
    print("Running FHW Join...")

    fhw = FHWJoin(relations, schemas)

    start_time = time.time()
    results = fhw.execute()
    execution_time = time.time() - start_time

    print(f"\nFHW Join completed in {execution_time:.6f} seconds")
    print(f"Number of results: {len(results)}")

    if results:
        print(f"\nSample results (first 10):")
        for t in sorted(results)[:10]:
            print(f"  {t}")
        if len(results) > 10:
            print(f"  ... and {len(results) - 10} more")

    print("\n" + "=" * 80)
    print("Algorithm Explanation:")
    print("=" * 80)
    print("""
Fractional Hypertree Width (FHW) Algorithm:

1. **Fractional Edge Cover**: For a set of variables X, assign fractional
   weights to relations such that each variable is "covered" with total weight ≥ 1

   For example, for variables {A1, A2, A3}:
   - A1 appears in R1 and R3
   - A2 appears in R1 and R2
   - A3 appears in R2 and R3

   A fractional cover could assign:
   - w(R1) = 0.5, w(R2) = 0.5, w(R3) = 0.5
   - Total = 1.5

   Each variable is covered:
   - A1: w(R1) + w(R3) = 0.5 + 0.5 = 1 ✓
   - A2: w(R1) + w(R2) = 0.5 + 0.5 = 1 ✓
   - A3: w(R2) + w(R3) = 0.5 + 0.5 = 1 ✓

2. **Fractional Hypertree Width**: The minimum ρ such that for every bag χ(t)
   in the decomposition, the fractional edge cover number is ≤ ρ

3. **Our Approach**:
   - Apply aggressive semi-join reductions (Yannakakis-style)
   - Join in an order respecting fractional covers
   - Triangle: ρ({A1,A2,A3}) = 1.5
   - Diamond: ρ({A4,A5,A6}) = 1.5
   - Overall FHW ≤ 1.5

4. **Execution Strategy**:
   a) Semi-join reduce triangle relations (R1, R2, R3)
   b) Join triangle
   c) Semi-join reduce R4, then join
   d) Semi-join reduce diamond relations (R5, R6, R7)
   e) Join diamond
   f) Final join

5. **Complexity**:
   - Fractional hypertree width: fhw ≈ 1.5 for this query
   - Running time: O(N^(fhw+1) + OUT) = O(N^2.5 + OUT)
   - Better than GHW when fhw < ghw
   - Semi-join reductions help in practice

6. **Advantages**:
   - More fine-grained than GHW
   - Semi-joins reduce relation sizes early
   - Can significantly improve practical performance
   - Still worst-case optimal for the FHW parameter

7. **Comparison**:
   - GenericJoin: General backtracking, O(N^ρ*)
   - GHW: Tree decomposition, O(N^(ghw+1))
   - FHW: Fractional covers + semi-joins, O(N^(fhw+1))
   - FHW ≤ GHW, so FHW can be better on cyclic queries
""")


if __name__ == "__main__":
    main()
