.PHONY: load process

load:
	clj -J-Xmx30000m -J-XX:+PrintGC -J-XX:+UseZGC -X load/run

process:
    clj -J-Xmx30000m -J-XX:+PrintGC -J-XX:+UseZGC -X process/run
