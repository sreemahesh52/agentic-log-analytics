# kafka package — KafkaHandler lives here.
# Isolating Kafka wiring from business logic enforces Single Responsibility:
# correlator.py handles the algorithm; handler.py handles I/O.
