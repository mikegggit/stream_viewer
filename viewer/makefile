bootRun:
	gradle bootRun --args='--start-time=14:22:22'

bootRunD:
	gradle bootRun --debug-jvm --args='--start-time=14:22:22'

kcg:
	kafka-consumer-groups.sh --bootstrap-server localhost:9092  --list
kcg2:
	kafka-consumer-groups.sh --bootstrap-server localhost:9092  --group SquirrelSessionConsumer1 --describe

kcc:
	kafka-console-consumer --bootstrap-server localhost:9092 --topic session-05-01-2019 --partition 14 --offset earliest

kcgresetdryrun:
	kafka-consumer-groups.sh --bootstrap-server localhost:9092  --group SquirrelSessionConsumer1 --reset-offsets --dry-run --topic session-05-01-2019 --to-earliest

kcgresetexecute:
	kafka-consumer-groups.sh --bootstrap-server localhost:9092  --group SquirrelSessionConsumer1 --reset-offsets --execute --topic session-05-01-2019 --to-earliest
