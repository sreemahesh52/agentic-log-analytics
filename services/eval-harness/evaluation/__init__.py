"""Faithfulness and hallucination evaluation package.
This package implements the three-tier faithfulness evaluation pipeline
using the Strategy pattern, plus an independent hallucination evaluator.
Strategy pipeline (tried in order, first non-None result wins):
  1. GroundTruthStrategy — most reliable; needs manual labelling
  2. SimilarityStrategy — medium reliability; uses ChromaDB past incidents
  3. HeuristicStrategy — always returns a result; final fallback
HallucinationEvaluator runs independently of the faithfulness pipeline
because hallucination and faithfulness are different concerns:
  - Faithfulness: does the conclusion match known truth?
  - Hallucination: does the conclusion invent claims not in the evidence?
"""
