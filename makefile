verify-zk-listening:
	nc -v localhost 2181

verify-kafkaConfig-listening:
	nc -v localhost 9092

lt:
	kafkaConfig-topics  --zookeeper localhost:2181 --list

tt:
	kafkaConfig-console-consumer --bootstrap-server localhost:9092 --topic MikeG --from-beginning

