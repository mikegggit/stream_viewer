verify-zk-listening:
	nc -v localhost 2181

verify-kafka-listening:
	nc -v localhost 9092

lt:
	kafka-topics  --zookeeper localhost:2181 --list

tt:
	kafka-console-consumer --bootstrap-server localhost:9092 --topic MikeG --from-beginning

