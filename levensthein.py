"""
This module provides an implementation of the Levenshtein distance algorithm.
"""

class Levenshtein:
    """
    A class for calculating the Levenshtein distance between two strings.
    """
    @staticmethod
    def distance(s1, s2):
        """
        Calculates the Levenshtein distance between two strings.

        The Levenshtein distance is the number of single-character edits
        (insertions, deletions, or substitutions) required to change one
        string into the other.

        Args:
            s1 (str): The first string.
            s2 (str): The second string.

        Returns:
            int: The Levenshtein distance between the two strings.
        """
        if len(s1) < len(s2):
            return Levenshtein.distance(s2, s1)

        previous_row = range(len(s2) + 1)
        for i, c1 in enumerate(s1):
            current_row = [i + 1]
            for j, c2 in enumerate(s2):
                insertions = previous_row[j + 1] + 1
                deletions = current_row[j] + 1
                substitutions = previous_row[j] + (c1 != c2)
                current_row.append(min(insertions, deletions, substitutions))
            previous_row = current_row

        return previous_row[-1]

if __name__ == '__main__':
    a = "Montrouge"
    b = "montchavin"
    ld = Levenshtein.distance(a, b)
    print(f"Levenshtein distance between '{a}' and '{b}' is {ld}")
