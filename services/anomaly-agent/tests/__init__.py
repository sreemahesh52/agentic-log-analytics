"""Tests package for the anomaly-agent service.
All tests in this package run with zero external dependencies:
  - No real Redis (fakeredis used instead)
  - No Kafka
  - No PostgreSQL
  - No OpenAI API calls
This makes the test suite runnable in CI with no infrastructure setup.
"""
