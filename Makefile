.PHONY: run

run:
	clj -J-Xmx30000m -J-XX:+PrintGC -J-XX:+UseZGC -X main/run
