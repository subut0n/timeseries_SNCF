all:	clean run

clean:
	if [ -e found_objects.db ]; then rm found_objects.db; fi

run:
	python3 import.py
