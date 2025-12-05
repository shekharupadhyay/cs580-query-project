"""
Problem 7: GenericJoin Algorithm

Query: q(A1, A2, A3, A4, A5, A6) :- R1(A1, A2), R2(A2, A3), R3(A1, A3),
                                     R4(A3, A4), R5(A4, A5), R6(A5, A6), R7(A4, A6)

This is a cyclic query with:
- Triangle: A1-A2-A3 (R1, R2, R3)
- Diamond: A4-A5-A6 (R5, R6, R7)

GenericJoin (Ngo, Porat, Ré, Rudra - 2012):
- Worst-case optimal join algorithm
- Running time: O(N^ρ* + OUT) where ρ* is the fractional edge cover number
- Uses a trie-based approach with backtracking
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


class GenericJoin:
    """
    GenericJoin algorithm for cyclic queries

    The algorithm works by:
    1. Building indexes for each relation
    2. Using a backtracking search with variable ordering
    3. Pruning branches early when no matches exist
    """

    def __init__(self, relations: Dict[str, List[Tuple[int, ...]]],
                 schemas: Dict[str, List[str]]):
        """
        Args:
            relations: Dict mapping relation name to list of tuples
            schemas: Dict mapping relation name to list of attribute names
        """
        self.relations = relations
        self.schemas = schemas

        self.indexes = self._build_indexes()

    def _build_indexes(self) -> Dict[str, Dict]:
        """
        Build trie-like indexes for each relation

        For a relation R(A, B, C), we build indexes:
        - {} -> all tuples
        - {A: a} -> tuples with A=a
        - {A: a, B: b} -> tuples with A=a and B=b
        """
        indexes = {}

        for rel_name, tuples in self.relations.items():
            schema = self.schemas[rel_name]
            rel_index = defaultdict(list)

            for t in tuples:
                assignment = {}
                for i, attr in enumerate(schema):
                    key = tuple(sorted(assignment.items()))
                    rel_index[key].append(t)
                    assignment[attr] = t[i]

                key = tuple(sorted(assignment.items()))
                rel_index[key].append(t)

            indexes[rel_name] = rel_index

        return indexes

    def _get_matching_tuples(self, rel_name: str, assignment: Dict[str, int]) -> List[Tuple[int, ...]]:
        """
        Get tuples from relation that match the current variable assignment
        """
        schema = self.schemas[rel_name]

        relevant_assignment = {var: val for var, val in assignment.items()
                              if var in schema}

        key = tuple(sorted(relevant_assignment.items()))

        if key in self.indexes[rel_name]:
            candidates = self.indexes[rel_name][key]
        else:
            candidates = []
            for t in self.relations[rel_name]:
                match = True
                for var, val in relevant_assignment.items():
                    idx = schema.index(var)
                    if t[idx] != val:
                        match = False
                        break
                if match:
                    candidates.append(t)

        result = []
        for t in candidates:
            valid = True
            for i, var in enumerate(schema):
                if var in assignment and t[i] != assignment[var]:
                    valid = False
                    break
            if valid:
                result.append(t)

        return result

    def _get_candidate_values(self, var: str, assignment: Dict[str, int]) -> Set[int]:
        """
        Get possible values for variable given current assignment

        Look at all relations containing var and intersect possible values
        """
        candidate_sets = []

        for rel_name, schema in self.schemas.items():
            if var not in schema:
                continue

            var_idx = schema.index(var)
            matching_tuples = self._get_matching_tuples(rel_name, assignment)

            if not matching_tuples:
                return set()

            candidates = {t[var_idx] for t in matching_tuples}
            candidate_sets.append(candidates)

        if not candidate_sets:
            return set()

        result = candidate_sets[0]
        for s in candidate_sets[1:]:
            result = result.intersection(s)

        return result

    def execute(self, output_vars: List[str]) -> List[Tuple[int, ...]]:
        """
        Execute GenericJoin to enumerate all solutions

        Args:
            output_vars: Variables to include in output (in order)

        Returns:
            List of output tuples
        """
        results = []

        all_vars = set()
        for schema in self.schemas.values():
            all_vars.update(schema)

        var_order = list(all_vars)

        def backtrack(var_idx: int, assignment: Dict[str, int]):
            """Backtracking search"""
            if var_idx == len(var_order):
                result_tuple = tuple(assignment[var] for var in output_vars)
                results.append(result_tuple)
                return

            var = var_order[var_idx]

            candidates = self._get_candidate_values(var, assignment)

            for value in candidates:
                assignment[var] = value
                backtrack(var_idx + 1, assignment)
                del assignment[var]

        backtrack(0, {})
        return results


def main():
    print("=" * 80)
    print("Problem 7: GenericJoin Algorithm")
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
    print("Running GenericJoin...")

    gj = GenericJoin(relations, schemas)

    start_time = time.time()
    results = gj.execute(['A1', 'A2', 'A3', 'A4', 'A5', 'A6'])
    execution_time = time.time() - start_time

    print(f"GenericJoin completed in {execution_time:.6f} seconds")
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
GenericJoin Algorithm:

1. **Variable Ordering**: Choose an order to assign variables (A1, A2, A3, A4, A5, A6)

2. **Backtracking Search**: For each variable in order:
   - Compute candidate values by intersecting domains from all relations
   - Only consider values that are consistent with current assignment
   - Prune branches early when no candidates exist

3. **Intersection-based Pruning**: For each variable X:
   - Look at all relations containing X
   - Find values of X consistent with current partial assignment
   - Take intersection of all such value sets
   - If intersection is empty, backtrack immediately

4. **Optimality**: The algorithm is worst-case optimal
   - Time: O(N^ρ* + OUT) where ρ* is fractional edge cover number
   - For this query with triangle and diamond, ρ* can be computed from the
     hypergraph structure

Query Structure:
- Triangle (A1, A2, A3): R1(A1,A2), R2(A2,A3), R3(A1,A3)
- Line (A3, A4, A5, A6): R4(A3,A4), R5(A4,A5), R6(A5,A6)
- Additional edge: R7(A4,A6) creates a diamond with A4-A5-A6

The fractional edge cover number ρ* for this query determines the
worst-case complexity. GenericJoin achieves this optimal bound.
""")


if __name__ == "__main__":
    main()
