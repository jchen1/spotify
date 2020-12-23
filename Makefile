.PHONY: run

run:
	clj -J-Xmx24000m -J-XX:+PrintGC -J-XX:+UseZGC -X main/run
