"""
Problem 7: Generalized Hypertree Width (GHW) Algorithm

Query: q(A1, A2, A3, A4, A5, A6) :- R1(A1, A2), R2(A2, A3), R3(A1, A3),
                                     R4(A3, A4), R5(A4, A5), R6(A5, A6), R7(A4, A6)

Generalized Hypertree Decomposition:
- Generalizes tree decompositions for hypergraphs
- Running time: O(N^(w+1) + OUT) where w is the generalized hypertree width
- Uses a tree decomposition where each node covers a subset of relations
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


class GHWJoin:
    """
    Generalized Hypertree Width Join Algorithm

    For this specific query, we manually construct a good hypertree decomposition
    and evaluate the query bottom-up on the decomposition tree.
    """

    def __init__(self, relations: Dict[str, List[Tuple[int, ...]]],
                 schemas: Dict[str, List[str]]):
        self.relations = relations
        self.schemas = schemas

    def _hash_join(self, left: List[Tuple], left_schema: List[str],
                   right: List[Tuple], right_schema: List[str]) -> Tuple[List[Tuple], List[str]]:
        """
        Hash join two relations on common attributes

        Returns: (joined tuples, joined schema)
        """

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

    def _project(self, tuples: List[Tuple], schema: List[str],
                 keep_attrs: List[str]) -> Tuple[List[Tuple], List[str]]:
        """Project relation onto specified attributes"""
        indices = [schema.index(attr) for attr in keep_attrs]

        projected = set()
        for t in tuples:
            proj_tuple = tuple(t[i] for i in indices)
            projected.add(proj_tuple)

        return list(projected), keep_attrs

    def execute(self) -> List[Tuple[int, ...]]:
        """
        Execute query using GHW decomposition

        For this query, we use a decomposition that respects the query structure:

        Tree structure:
                    Node1 {R1, R2, R3}  -- vars: {A1, A2, A3}
                        |
                    Node2 {R4}          -- vars: {A3, A4}
                        |
                    Node3 {R5, R7}      -- vars: {A4, A5, A6}
                        |
                    Node4 {R6}          -- vars: {A5, A6}

        This is a chain decomposition with width ≤ 2
        """

        print("  Node 1: Joining R1, R2, R3 (triangle)...")
        temp1, schema1 = self._hash_join(
            self.relations['R1'], self.schemas['R1'],
            self.relations['R2'], self.schemas['R2']
        )
        temp1, schema1 = self._hash_join(
            temp1, schema1,
            self.relations['R3'], self.schemas['R3']
        )
        print(f"    Result: {len(temp1)} tuples, schema: {schema1}")

        print("  Node 2: Joining with R4...")
        temp2, schema2 = self._hash_join(
            temp1, schema1,
            self.relations['R4'], self.schemas['R4']
        )
        print(f"    Result: {len(temp2)} tuples, schema: {schema2}")

        print("  Node 3: Joining R5 and R7...")
        temp3_sub, schema3_sub = self._hash_join(
            self.relations['R5'], self.schemas['R5'],
            self.relations['R7'], self.schemas['R7']
        )
        print(f"    R5 ⋈ R7: {len(temp3_sub)} tuples, schema: {schema3_sub}")

        temp3, schema3 = self._hash_join(
            temp2, schema2,
            temp3_sub, schema3_sub
        )
        print(f"    Result: {len(temp3)} tuples, schema: {schema3}")

        print("  Node 4: Joining with R6...")
        temp4, schema4 = self._hash_join(
            temp3, schema3,
            self.relations['R6'], self.schemas['R6']
        )
        print(f"    Result: {len(temp4)} tuples, schema: {schema4}")

        output_attrs = ['A1', 'A2', 'A3', 'A4', 'A5', 'A6']
        result, _ = self._project(temp4, schema4, output_attrs)

        return result


def main():
    print("=" * 80)
    print("Problem 7: Generalized Hypertree Width (GHW) Algorithm")
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
    print("Running GHW Join...")

    ghw = GHWJoin(relations, schemas)

    start_time = time.time()
    results = ghw.execute()
    execution_time = time.time() - start_time

    print(f"\nGHW Join completed in {execution_time:.6f} seconds")
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
Generalized Hypertree Width (GHW) Algorithm:

1. **Hypertree Decomposition**: Construct a tree where each node contains:
   - χ(t): A set of variables (bag)
   - λ(t): A set of relations covered by this node

   Properties:
   - Each relation appears in at least one node
   - For each variable X, nodes containing X form a connected subtree
   - Each node can "guard" its variables using the relations it covers

2. **Width**: The generalized hypertree width is the maximum size of χ(t)
   over all nodes t in the decomposition

3. **Our Decomposition**:

   Node 1: {R1, R2, R3}
   - Variables: {A1, A2, A3}
   - Join the triangle first

   Node 2: {R4}
   - Variables: {A3, A4}
   - Extend with A4

   Node 3: {R5, R7}
   - Variables: {A4, A5, A6}
   - Join the diamond relations

   Node 4: {R6}
   - Variables: {A5, A6}
   - Complete the join

4. **Execution**: Bottom-up on the decomposition tree
   - Process each node by joining its relations
   - Pass results up the tree
   - Join with parent node

5. **Complexity**:
   - Width of this decomposition: w = 3 (Node 1 and Node 3 have 3 variables)
   - Running time: O(N^(w+1) + OUT) = O(N^4 + OUT)
   - For N=100: O(100^4) = O(100,000,000) in the worst case
   - Actual performance depends on intermediate result sizes

6. **Comparison to GenericJoin**:
   - GenericJoin: O(N^ρ* + OUT) where ρ* is fractional edge cover
   - GHW: O(N^(w+1) + OUT) where w is hypertree width
   - Both are worst-case optimal for different parameterizations
   - Performance depends on query structure and data distribution
""")


if __name__ == "__main__":
    main()
