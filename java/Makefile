default : all

all : mapreduce.class

mapreduce.class : mapreduce.java
	sleep 1
	javac $<

run : mapreduce.class
	rm ./in/* ; ./split "large.txt" 4; mv input_* in
	# rm ./in/* ; ./split "small.txt" 1; mv input_* in
	# java mapreduce
	time -v java mapreduce

.PHONY: clean
clean :
	rm -rf *.class
